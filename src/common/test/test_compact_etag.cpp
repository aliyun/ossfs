/*
 * Copyright 2025 The Ossfs Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compact_etag.h"
#include "fs/staged_inode_cache.h"

using namespace OssFileSystem;

class CompactEtagTest : public ::testing::Test {
 protected:
  static constexpr int kEmpty = static_cast<int>(CompactEtag::Kind::kEmpty);
  static constexpr int kNormal = static_cast<int>(CompactEtag::Kind::kNormal);
  static constexpr int kMultipart =
      static_cast<int>(CompactEtag::Kind::kMultipart);
  static constexpr int kOther = static_cast<int>(CompactEtag::Kind::kOther);

  static int KindOf(const CompactEtag &e) {
    return static_cast<int>(e.kind());
  }

  static const char *KindName(int k) {
    switch (k) {
      case kEmpty:
        return "kEmpty";
      case kNormal:
        return "kNormal";
      case kMultipart:
        return "kMultipart";
      case kOther:
        return "kOther";
    }
    return "unknown";
  }

  static void ExpectKind(const CompactEtag &e, int want_kind,
                         const std::string &input) {
    EXPECT_EQ(KindOf(e), want_kind)
        << "input=" << testing::PrintToString(input) << ", got kind "
        << KindName(KindOf(e)) << ", want " << KindName(want_kind);
  }

  // Asserts the storage kind and the full round-trip / equality contract for
  // an input that is already in canonical form (to_string() == input).
  static void Expect(const std::string &input, int want_kind) {
    CompactEtag e(input);
    ExpectKind(e, want_kind, input);
    EXPECT_EQ(e.to_string(), input)
        << "round-trip " << testing::PrintToString(input);
    EXPECT_EQ(e.size(), input.size());
    EXPECT_EQ(e.empty(), input.empty());
    EXPECT_TRUE(e.equals(input));

    // Copy must preserve the value.
    CompactEtag copy(e);
    EXPECT_EQ(copy.to_string(), input);
    EXPECT_TRUE(copy.equals(e));
    EXPECT_TRUE(e.equals(copy));

    // Move must preserve the value and reset the source to empty.
    CompactEtag moved(std::move(e));
    EXPECT_TRUE(e.empty());
    EXPECT_EQ(moved.to_string(), input);
    EXPECT_TRUE(moved.equals(input));
  }

  // Asserts that a case-variant hex input normalizes to its canonical
  // (uppercase) form while keeping the same identity.
  static void ExpectNormalized(const std::string &input, int want_kind,
                               const std::string &canonical) {
    CompactEtag e(input);
    ExpectKind(e, want_kind, input);
    EXPECT_EQ(e.to_string(), canonical)
        << "normalize " << testing::PrintToString(input);
    EXPECT_EQ(e.size(), input.size());
    // Equality is case-insensitive for hex letters, in both directions.
    EXPECT_TRUE(e.equals(input));
    EXPECT_TRUE(e.equals(canonical));

    CompactEtag copy(e);
    EXPECT_EQ(copy.to_string(), canonical);
    EXPECT_TRUE(copy.equals(e));

    CompactEtag moved(std::move(e));
    EXPECT_TRUE(e.empty());
    EXPECT_EQ(moved.to_string(), canonical);
    EXPECT_TRUE(moved.equals(input));
  }
};

namespace {

constexpr char kHexLower[] = "0123456789abcdef0123456789abcdef";
constexpr char kHexUpper[] = "0123456789ABCDEF0123456789ABCDEF";
constexpr char kHexMixed[] = "0123456789aBcDeF0123456789AbCdEf";

std::string make_legacy_source_key(const std::string &object_key,
                                   const std::string &etag, timespec mtime,
                                   size_t size) {
  if (etag.empty()) {
    return object_key + "/" + std::to_string(size) + "@" +
           std::to_string(mtime.tv_sec) + "." + std::to_string(mtime.tv_nsec);
  }
  return object_key + "/" + etag;
}

std::string make_current_source_key(const std::string &object_key,
                                    const CompactEtag &etag, timespec mtime,
                                    size_t size) {
  if (etag.empty()) {
    return object_key + "/" + std::to_string(size) + "@" +
           std::to_string(mtime.tv_sec) + "." + std::to_string(mtime.tv_nsec);
  }
  std::string result(object_key);
  result.push_back('/');
  etag.append_to(&result);
  return result;
}

TEST_F(CompactEtagTest, empty) {
  CompactEtag e;
  EXPECT_TRUE(e.empty());
  EXPECT_EQ(e.size(), 0u);
  EXPECT_EQ(e.to_string(), "");
  EXPECT_TRUE(e.equals(""));
  EXPECT_FALSE(e.equals("x"));
  Expect("", kEmpty);
}

TEST_F(CompactEtagTest, short_values_use_other_kind) {
  Expect("a", kOther);
  Expect(std::string(15, 'x'), kOther);
  Expect(std::string(16, 'x'), kOther);
  Expect(std::string(21, 'x'), kOther);
  Expect(std::string(22, 'x'), kOther);
  // 22 bytes with surrounding quotes but a non-32-hex inner part.
  Expect("\"aaaaaaaaaaaaaaaaaaaa\"", kOther);
  Expect("\"etag-a\"", kOther);
  Expect("\"\"", kOther);
  Expect("W/\"ab\"", kOther);
}

TEST_F(CompactEtagTest, other_boundaries) {
  // Values that do not match a normal form use kOther.
  Expect(std::string(23, 'x'), kOther);
  Expect(std::string(64, 'e'), kOther);
  Expect(std::string(256, 'q'), kOther);
}

TEST_F(CompactEtagTest, unquoted_hex_uses_other_kind) {
  for (const char *hex : {kHexUpper, kHexLower, kHexMixed}) {
    for (const char *suffix : {"", "-0", "-1", "-10", "-9999", "-10000"}) {
      Expect(std::string(hex) + suffix, kOther);
    }
  }
  EXPECT_TRUE(CompactEtag(kHexLower).equals(kHexUpper));
  EXPECT_TRUE(CompactEtag(kHexUpper).equals(kHexLower));
  EXPECT_TRUE(CompactEtag(kHexLower).equals(CompactEtag(kHexUpper)));
  EXPECT_TRUE(CompactEtag(std::string(kHexLower) + "-1")
                  .equals(std::string(kHexUpper) + "-1"));
}

TEST_F(CompactEtagTest, normal_etag) {
  Expect(std::string("\"") + kHexUpper + "\"", kNormal);
  ExpectNormalized(std::string("\"") + kHexLower + "\"", kNormal,
                   std::string("\"") + kHexUpper + "\"");
  ExpectNormalized(std::string("\"") + kHexMixed + "\"", kNormal,
                   std::string("\"") + kHexUpper + "\"");
}

TEST_F(CompactEtagTest, multipart_etag) {
  for (const char *suffix : {"-0", "-1", "-9", "-10", "-99", "-100", "-999",
                             "-1000", "-9999", "-10000"}) {
    std::string canonical = std::string("\"") + kHexUpper + suffix + "\"";
    Expect(canonical, kMultipart);
    ExpectNormalized(std::string("\"") + kHexLower + suffix + "\"", kMultipart,
                     canonical);
    ExpectNormalized(std::string("\"") + kHexMixed + suffix + "\"", kMultipart,
                     canonical);
  }
}

TEST_F(CompactEtagTest, multipart_zero_part_count) {
  const std::string canonical = std::string("\"") + kHexUpper + "-0\"";
  Expect(canonical, kMultipart);
  ExpectNormalized(std::string("\"") + kHexLower + "-0\"", kMultipart,
                   canonical);
  ExpectNormalized(std::string("\"") + kHexMixed + "-0\"", kMultipart,
                   canonical);
  EXPECT_FALSE(
      CompactEtag(canonical).equals(std::string("\"") + kHexUpper + "-00\""));
}

TEST_F(CompactEtagTest, noncanonical_part_count_uses_other_kind) {
  for (const char *suffix : {"-00", "-0001", "-10001", "-100000", "-", "-abc",
                             "-+1", "--1", "-1 "}) {
    std::string raw = std::string(kHexLower) + suffix;
    Expect(raw, kOther);
    Expect("\"" + raw + "\"", kOther);
  }
}

TEST_F(CompactEtagTest, other_fallback) {
  // 32 non-hex characters.
  Expect("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", kOther);
  // 33 chars: one past hex32 length.
  Expect(std::string(kHexLower) + "g", kOther);
  // Quoted but inner part contains a non-hex char.
  Expect("\"0123456789abcdef0123456789abcdeg\"", kOther);
  // Long weak validator.
  Expect(std::string("W/\"") + kHexLower + "\"", kOther);
  // Way beyond any fixed-capacity inline holder (e.g. 47 bytes).
  Expect(std::string(48, 'a'), kOther);
  Expect(std::string(4096, 'f'), kOther);
  // Embedded NUL must survive the round-trip.
  Expect(std::string("ab\0cd", 5), kOther);
  Expect(std::string("0123456789abcdef01234567\0abcdef-", 32), kOther);
  // Mismatched quotes are not treated as quoted.
  Expect("\"abc", kOther);
  Expect("abc\"", kOther);
  Expect("\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", kOther);
  Expect("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"", kOther);
}

TEST_F(CompactEtagTest, quoted_comparison_preserves_identity) {
  for (const char *suffix : {"", "-1", "-10000"}) {
    std::string upper = std::string(kHexUpper) + suffix;
    std::string quoted_upper = "\"" + upper + "\"";
    std::string quoted_lower = std::string("\"") + kHexLower + suffix + "\"";
    std::string quoted_mixed = std::string("\"") + kHexMixed + suffix + "\"";
    CompactEtag compressed(quoted_upper);
    EXPECT_TRUE(compressed.equals(quoted_lower));
    EXPECT_TRUE(compressed.equals(quoted_mixed));
    EXPECT_TRUE(CompactEtag(quoted_lower).equals(quoted_upper));
    EXPECT_TRUE(compressed.equals(CompactEtag(quoted_lower)));
    EXPECT_TRUE(CompactEtag(quoted_lower).equals(compressed));

    CompactEtag unquoted(upper);
    EXPECT_FALSE(compressed.equals(upper));
    EXPECT_FALSE(unquoted.equals(quoted_upper));
    EXPECT_FALSE(compressed.equals(unquoted));
    EXPECT_FALSE(unquoted.equals(compressed));

    for (const std::string &raw :
         {"x" + upper + "x", "\"" + upper + "x", "x" + upper + "\""}) {
      EXPECT_FALSE(compressed.equals(raw));
      EXPECT_FALSE(compressed.equals(CompactEtag(raw)));
      EXPECT_FALSE(CompactEtag(raw).equals(compressed));
    }
  }
  std::string quoted = std::string("\"") + kHexUpper + "\"";
  std::string with_suffix = std::string("\"") + kHexUpper + "-1\"";
  EXPECT_FALSE(CompactEtag(quoted).equals(with_suffix));
  EXPECT_FALSE(CompactEtag(quoted).equals(CompactEtag(with_suffix)));
  EXPECT_FALSE(
      CompactEtag(with_suffix).equals(std::string("\"") + kHexUpper + "-01\""));
  EXPECT_FALSE(
      CompactEtag(with_suffix).equals(std::string("\"") + kHexUpper + "-2\""));
  EXPECT_FALSE(
      CompactEtag(quoted).equals("\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\""));
}

TEST_F(CompactEtagTest, other_comparison_folds_only_hex_letters) {
  const std::vector<std::pair<std::string, std::string>> equal_cases = {
      {"AbC", "abc"},
      {"ABCDEF", "abcdef"},
      {std::string("\"") + kHexLower + "-0001\"",
       std::string("\"") + kHexUpper + "-0001\""},
      {std::string("Ab\0Ce", 5), std::string("aB\0cE", 5)},
  };
  for (const auto &[raw_a, raw_b] : equal_cases) {
    CompactEtag a(raw_a);
    CompactEtag b(raw_b);
    ExpectKind(a, kOther, raw_a);
    ExpectKind(b, kOther, raw_b);
    EXPECT_TRUE(a.equals(raw_b));
    EXPECT_TRUE(b.equals(raw_a));
    EXPECT_TRUE(a.equals(b));
    EXPECT_TRUE(b.equals(a));
    EXPECT_EQ(a.to_string(), raw_a);
    EXPECT_EQ(b.to_string(), raw_b);
    std::string output = "prefix/";
    a.append_to(&output);
    EXPECT_EQ(output, "prefix/" + raw_a);
  }

  const std::vector<std::pair<std::string, std::string>> unequal_cases = {
      {"G", "g"},
      {"VersionA", "versiona"},
      {"W/\"AbC\"", "w/\"abc\""},
      {"opaque-abcdef", "OPAQUE-ABCDEF"},
      {std::string("Ab\0Gz", 5), std::string("aB\0gZ", 5)},
  };
  for (const auto &[raw_a, raw_b] : unequal_cases) {
    CompactEtag a(raw_a);
    CompactEtag b(raw_b);
    EXPECT_FALSE(a.equals(raw_b));
    EXPECT_FALSE(b.equals(raw_a));
    EXPECT_FALSE(a.equals(b));
    EXPECT_FALSE(b.equals(a));
  }

  EXPECT_FALSE(CompactEtag("\"\"").equals(""));
  EXPECT_FALSE(CompactEtag("AbC").equals("\"abc\""));
  EXPECT_FALSE(CompactEtag("AbC").equals("abcd"));
  EXPECT_FALSE(CompactEtag("AbC").equals("abd"));
  EXPECT_FALSE(CompactEtag("opaque-1").equals("OPAQUE-01"));
  EXPECT_FALSE(
      CompactEtag(std::string(1, '\xC0')).equals(std::string(1, '\xE0')));
}

TEST_F(CompactEtagTest, copy_move_lifecycle) {
  const std::string normal_val = std::string("\"") + kHexUpper + "\"";
  const std::string other_val = std::string(64, 'z');

  // copy ctor
  CompactEtag a(normal_val);
  CompactEtag a_copy(a);
  EXPECT_EQ(a_copy.to_string(), normal_val);
  CompactEtag b(other_val);
  CompactEtag b_copy(b);
  EXPECT_EQ(b_copy.to_string(), other_val);

  // Copy assignment across normal and other representations.
  CompactEtag c(normal_val);
  c = b;
  EXPECT_EQ(c.to_string(), other_val);
  CompactEtag d(other_val);
  d = a;
  EXPECT_EQ(d.to_string(), normal_val);

  // Self copy assignment for a heap value.
  CompactEtag self_c(other_val);
  self_c = self_c;
  EXPECT_EQ(self_c.to_string(), other_val);

  // move ctor / move assign: source becomes empty, value transfers
  CompactEtag m1(std::move(b));
  EXPECT_TRUE(b.empty());
  EXPECT_EQ(m1.to_string(), other_val);
  CompactEtag m2(normal_val);
  m2 = std::move(m1);
  EXPECT_TRUE(m1.empty());
  EXPECT_EQ(m2.to_string(), other_val);

  // self move assignment
  CompactEtag self_m(other_val);
  auto &self_m_alias = self_m;
  self_m = std::move(self_m_alias);
  EXPECT_EQ(self_m.to_string(), other_val);

  // clear + reuse
  CompactEtag x(other_val);
  x.clear();
  EXPECT_TRUE(x.empty());
  x.assign(normal_val);
  EXPECT_EQ(x.to_string(), normal_val);
  x.assign(other_val);
  EXPECT_EQ(x.to_string(), other_val);
}

TEST_F(CompactEtagTest, assign_across_storage_kinds) {
  const std::vector<std::string> values = {
      "",
      std::string("\"") + kHexUpper + "\"",
      std::string("\"") + kHexUpper + "-7\"",
      "x",
      "\"\"",
      kHexLower,
      std::string(64, 'z')};
  for (const auto &from : values) {
    for (const auto &to : values) {
      CompactEtag source(to);
      CompactEtag target(from);
      target = source;
      source.clear();
      EXPECT_TRUE(target.equals(to));
      EXPECT_EQ(target.to_string(), to);

      target.assign(from);
      source.assign(to);
      target = std::move(source);
      EXPECT_TRUE(source.empty());
      EXPECT_TRUE(target.equals(to));
      EXPECT_EQ(target.to_string(), to);

      target.assign(from);
      target.assign(to);
      EXPECT_TRUE(target.equals(CompactEtag(to)));
      EXPECT_EQ(target.to_string(), to);
    }
  }
}

TEST_F(CompactEtagTest, assign_from_raw_string) {
  CompactEtag e("first");
  std::string raw = std::string("\"") + kHexLower + "-7\"";
  std::string canonical = std::string("\"") + kHexUpper + "-7\"";
  e.assign(raw);
  EXPECT_EQ(e.to_string(), canonical);
  e.assign(std::string_view(raw));
  EXPECT_TRUE(e.equals(raw));
  EXPECT_TRUE(e.equals(canonical));
  // Assigning from own textual form must be safe.
  e.assign(e.to_string());
  EXPECT_EQ(e.to_string(), canonical);
  // String literals convert to std::string_view implicitly.
  e.assign("\"etag-a\"");
  EXPECT_TRUE(e.equals("\"etag-a\""));
}

TEST_F(CompactEtagTest, api_contract) {
  // Equality is called under inode/cache locks and in verified refill.
  static_assert(noexcept(std::declval<const CompactEtag &>().equals(
                    std::declval<std::string_view>())),
                "equals(raw) must be noexcept");
  static_assert(noexcept(std::declval<const CompactEtag &>().equals(
                    std::declval<const CompactEtag &>())),
                "equals(compact) must be noexcept");
  static_assert(!std::is_convertible<std::string_view, CompactEtag>::value,
                "raw etag must not implicitly convert");
  SUCCEED();
}

// Native OSS ETags are already quoted uppercase hex, so their source keys
// remain byte-identical to the pre-CompactEtag representation.
TEST_F(CompactEtagTest, native_oss_source_key_compat) {
  timespec mtime{1700000000, 123456789};
  const std::vector<std::string> etags = {
      std::string("\"") + kHexUpper + "\"",
      std::string("\"") + kHexUpper + "-10000\"",
      "",
  };
  for (const auto &etag : etags) {
    EXPECT_EQ(
        make_legacy_source_key("dir/file", etag, mtime, 4096),
        make_current_source_key("dir/file", CompactEtag(etag), mtime, 4096))
        << "etag=" << testing::PrintToString(etag);
  }
}

// Serialization preserves opaque bytes but canonicalizes compressed quoted
// hex values to uppercase. This test defines the source-key policy; it does not
// claim byte compatibility for noncanonical quoted-hex input.
TEST_F(CompactEtagTest, disk_cache_source_key_serialization) {
  timespec mtime{1700000000, 123456789};
  // {input etag, etag bytes expected in the source key}.
  const std::vector<std::pair<std::string, std::string>> cases = {
      {kHexUpper, kHexUpper},
      {kHexLower, kHexLower},
      {kHexMixed, kHexMixed},
      {std::string(kHexLower) + "-10000", std::string(kHexLower) + "-10000"},
      {std::string("\"") + kHexLower + "\"",
       std::string("\"") + kHexUpper + "\""},
      {std::string("\"") + kHexMixed + "-10000\"",
       std::string("\"") + kHexUpper + "-10000\""},
      {"\"\"", "\"\""},
      {std::string("W/\"") + kHexLower + "\"",
       std::string("W/\"") + kHexLower + "\""},
      {std::string(64, 'z'), std::string(64, 'z')},
  };
  for (const auto &[input, key_form] : cases) {
    EXPECT_EQ(
        make_legacy_source_key("dir/file", key_form, mtime, 4096),
        make_current_source_key("dir/file", CompactEtag(input), mtime, 4096))
        << "etag=" << testing::PrintToString(input);
  }
}

// Deterministic random property test: any input must round-trip and compare
// equal to itself. The charset has no lowercase a-f letters so every input
// is already canonical; case normalization is covered by directed tests.
TEST_F(CompactEtagTest, random_round_trip) {
  uint32_t state = 0x9e3779b9;
  auto next_u32 = [&state]() {
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    return state;
  };
  // Charset heavy on hex digits, quotes, dashes and 'W' to stress the
  // parser's format boundaries. Lowercase x/g/z cover non-hex letters.
  static const char charset[] = "0123456789ABCDEF-\"W/xgz";
  static const int charset_len = sizeof(charset) - 1;

  for (int iter = 0; iter < 20000; iter++) {
    uint32_t r = next_u32() % 100;
    size_t len;
    if (r < 10) {
      len = next_u32() % 3;
    } else if (r < 30) {
      len = next_u32() % 11 + 31;
    } else {
      len = next_u32() % 80;  // 0..79
    }
    std::string input(len, '\0');
    for (size_t i = 0; i < len; i++) {
      input[i] = charset[next_u32() % charset_len];
    }
    CompactEtag e(input);
    ASSERT_EQ(e.to_string(), input) << "iter=" << iter;
    ASSERT_TRUE(e.equals(input)) << "iter=" << iter;
  }
}

TEST_F(CompactEtagTest, random_case_normalization) {
  uint32_t state = 0xabcdef01;
  auto next_u32 = [&state]() {
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    return state;
  };
  static const char lower_hex[] = "0123456789abcdef";

  for (int iter = 0; iter < 5000; iter++) {
    std::string input;
    std::string canonical;
    if (next_u32() % 3 == 0) {
      input.push_back('"');
      canonical.push_back('"');
    }
    for (int i = 0; i < 32; i++) {
      char c = lower_hex[next_u32() % 16];
      if (c >= 'a' && (next_u32() % 2)) c = static_cast<char>(c - 'a' + 'A');
      input.push_back(c);
      canonical.push_back(
          (c >= 'a' && c <= 'f') ? static_cast<char>(c - 'a' + 'A') : c);
    }
    if (next_u32() % 2) {
      unsigned suffix = next_u32() % 10000 + 1;
      input.push_back('-');
      canonical.push_back('-');
      input += std::to_string(suffix);
      canonical += std::to_string(suffix);
    }
    if (input.front() == '"') {
      input.push_back('"');
      canonical.push_back('"');
    }

    CompactEtag e(input);
    const std::string &expected = input.front() == '"' ? canonical : input;
    ASSERT_EQ(e.to_string(), expected) << "iter=" << iter << " input=" << input;
    ASSERT_TRUE(e.equals(input)) << "iter=" << iter;
    ASSERT_TRUE(e.equals(canonical)) << "iter=" << iter;
    ASSERT_TRUE(e.equals(CompactEtag(canonical))) << "iter=" << iter;
    ASSERT_TRUE(CompactEtag(canonical).equals(input)) << "iter=" << iter;
  }
}

// The staged cache stores CompactEtag and lookup moves it out of the
// consumed entry: the etag identity must survive insert/find intact for
// normal, multipart and other forms; the moved-from entry must be
// empty.
TEST(StagedCacheEtagTest, etag_round_trip_through_cache) {
  StagedInodeCache cache(3600);
  // {input etag, expected normalized text after the move hand-off}.
  const std::vector<std::pair<std::string, std::string>> cases = {
      {std::string("\"") + kHexLower + "\"",
       std::string("\"") + kHexUpper + "\""},  // normal (case-normalized)
      {std::string("\"") + kHexMixed + "-777\"",
       std::string("\"") + kHexUpper + "-777\""},
      {kHexLower, kHexLower},
      {std::string(kHexMixed) + "-777", std::string(kHexMixed) + "-777"},
      {"x", "x"},
      {"\"\"", "\"\""},
      {std::string("opaque-") + std::string(50, 'y'),
       std::string("opaque-") + std::string(50, 'y')},  // other verbatim
      {"", ""},                                         // empty
  };
  for (size_t i = 0; i < cases.size(); i++) {
    std::string name = "f" + std::to_string(i);
    cache.insert(1, name, 0, timespec{}, CompactEtag(cases[i].first), i + 2,
                 InodeType::kFile, time(nullptr));
  }
  for (size_t i = 0; i < cases.size(); i++) {
    std::string name = "f" + std::to_string(i);
    std::unique_ptr<StagedInodeCache::CacheEntry> entry;
    ASSERT_TRUE(cache.find_and_erase(1, name, &entry));
    // Same hand-off as lookup_from_staged_cache_if_enabled().
    CompactEtag moved = std::move(entry->etag);
    EXPECT_TRUE(entry->etag.empty());
    EXPECT_TRUE(moved.equals(cases[i].first))
        << "etag=" << testing::PrintToString(cases[i].first);
    EXPECT_EQ(moved.to_string(), cases[i].second);
  }
}

}  // namespace
