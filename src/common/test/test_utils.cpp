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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>

#include "common/logger.h"
#include "common/utils.h"

TEST(CommonUtilsTest, trim_string_view) {
  // Test basic trimming of whitespace
  EXPECT_EQ(trim_string_view("  hello  "), "hello");

  // Test trimming with custom predicate
  auto is_vowel = [](char c) {
    return c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u';
  };
  EXPECT_EQ(trim_string_view("aaihelloou", is_vowel), "hell");

  // Test empty string
  EXPECT_EQ(trim_string_view(""), "");

  // Test string with only spaces
  EXPECT_EQ(trim_string_view("   "), "");

  // Test string without leading/trailing spaces
  EXPECT_EQ(trim_string_view("hello"), "hello");

  // Test string with only leading spaces
  EXPECT_EQ(trim_string_view("   hello"), "hello");

  // Test string with only trailing spaces
  EXPECT_EQ(trim_string_view("hello   "), "hello");

  // Test with tabs and newlines
  EXPECT_EQ(trim_string_view("\t\n hello \n\t"), "hello");
}

TEST(CommonUtilsTest, parse_bytes_string) {
  // Test basic values without units
  EXPECT_EQ(parse_bytes_string("1234").value(), 1234ULL);
  EXPECT_EQ(parse_bytes_string("0").value(), 0ULL);

  // Test with whitespace
  EXPECT_EQ(parse_bytes_string(" 1234 ").value(), 1234ULL);

  // Test kilobytes
  EXPECT_EQ(parse_bytes_string("1K").value(), 1ULL << 10);
  EXPECT_EQ(parse_bytes_string("2KB").value(), 2ULL << 10);
  EXPECT_EQ(parse_bytes_string("1k").value(), 1ULL << 10);
  EXPECT_EQ(parse_bytes_string("1Ki").value(), 1ULL << 10);

  // Test megabytes
  EXPECT_EQ(parse_bytes_string("1M").value(), 1ULL << 20);
  EXPECT_EQ(parse_bytes_string("3MB").value(), 3ULL << 20);
  EXPECT_EQ(parse_bytes_string("1m").value(), 1ULL << 20);
  EXPECT_EQ(parse_bytes_string("1Mi").value(), 1ULL << 20);

  // Test gigabytes
  EXPECT_EQ(parse_bytes_string("1G").value(), 1ULL << 30);
  EXPECT_EQ(parse_bytes_string("2GB").value(), 2ULL << 30);
  EXPECT_EQ(parse_bytes_string("1g").value(), 1ULL << 30);
  EXPECT_EQ(parse_bytes_string("111Gi").value(), 111ULL << 30);

  // Test terabytes
  EXPECT_EQ(parse_bytes_string("1T").value(), 1ULL << 40);
  EXPECT_EQ(parse_bytes_string("2TB").value(), 2ULL << 40);
  EXPECT_EQ(parse_bytes_string("1t").value(), 1ULL << 40);
  EXPECT_EQ(parse_bytes_string("1Ti").value(), 1ULL << 40);

  // Test petabytes
  EXPECT_EQ(parse_bytes_string("1P").value(), 1ULL << 50);
  EXPECT_EQ(parse_bytes_string("2PB").value(), 2ULL << 50);
  EXPECT_EQ(parse_bytes_string("16p").value(), 16ULL << 50);
  EXPECT_EQ(parse_bytes_string("1Pi").value(), 1ULL << 50);

  // Test invalid cases
  EXPECT_FALSE(parse_bytes_string("").has_value());
  EXPECT_FALSE(parse_bytes_string("abc").has_value());
  EXPECT_FALSE(parse_bytes_string("123XYZ").has_value());
  EXPECT_FALSE(parse_bytes_string("-123").has_value());
  EXPECT_FALSE(parse_bytes_string("K").has_value());
  EXPECT_FALSE(parse_bytes_string("1 K").has_value());

  // Test overflow case
  EXPECT_FALSE(
      parse_bytes_string("184467440737095516151P").has_value());  // Too large
}

TEST(CommonUtilsTest, split_command_tokens) {
  std::vector<std::string> tokens;

  // Test basic command splitting
  EXPECT_EQ(split_command_tokens("ls -l /tmp", tokens), 0);
  EXPECT_EQ(tokens.size(), size_t(3));
  EXPECT_EQ(tokens[0], "ls");
  EXPECT_EQ(tokens[1], "-l");
  EXPECT_EQ(tokens[2], "/tmp");

  // Test command with quotes
  EXPECT_EQ(split_command_tokens("echo \"hello world\"", tokens), 0);
  EXPECT_EQ(tokens.size(), size_t(2));
  EXPECT_EQ(tokens[0], "echo");
  EXPECT_EQ(tokens[1], "hello world");

  // Test command with binary containing spaces
  EXPECT_EQ(split_command_tokens("\"my echo\" \"hello world\"", tokens), 0);
  EXPECT_EQ(tokens.size(), size_t(2));
  EXPECT_EQ(tokens[0], "my echo");
  EXPECT_EQ(tokens[1], "hello world");

  // Test command with escaped characters
  EXPECT_EQ(split_command_tokens("echo hello\\ world", tokens), 0);
  EXPECT_EQ(tokens.size(), size_t(2));
  EXPECT_EQ(tokens[0], "echo");
  EXPECT_EQ(tokens[1], "hello world");

  // Test empty command
  EXPECT_EQ(split_command_tokens("", tokens), 0);
  EXPECT_TRUE(tokens.empty());

  // Test invalid command (with unsupported command substitution)
  EXPECT_LT(split_command_tokens("echo $(ls)", tokens), 0);
  EXPECT_LT(split_command_tokens("echo `ls`", tokens), 0);
  EXPECT_LT(split_command_tokens("echo 123 | tail -n", tokens), 0);
  EXPECT_LT(split_command_tokens("echo 123 >> file", tokens), 0);
}

TEST(CommonUtilsTest, run_process_safe) {
  std::string stdout_output, stderr_output;

  // Test successful command execution
  std::vector<std::string> args = {"/bin/echo", "hello"};
  int result = run_process_safe(args, stdout_output, stderr_output, 1024);
  EXPECT_EQ(result, 0);
  EXPECT_EQ(stdout_output, "hello\n");
  EXPECT_TRUE(stderr_output.empty());

  // Test command with stderr output
  std::vector<std::string> error_args = {"/bin/sh", "-c", "echo error >&2"};
  result = run_process_safe(error_args, stdout_output, stderr_output, 1024);
  EXPECT_EQ(result, 0);
  EXPECT_TRUE(stdout_output.empty());
  EXPECT_EQ(stderr_output, "error\n");

  // Test non-existent command
  std::vector<std::string> nonexistent_args = {"/nonexistent/command"};
  result =
      run_process_safe(nonexistent_args, stdout_output, stderr_output, 1024);
  EXPECT_NE(result, 0);

  // Test command with output size limit
  std::vector<std::string> long_output_args = {"/bin/sh", "-c",
                                               "printf '%s' $(seq 1 1000)"};
  result =
      run_process_safe(long_output_args, stdout_output, stderr_output, 100);
  EXPECT_EQ(result, -ENOMEM);  // Output was truncated

  // Test empty arguments
  std::vector<std::string> empty_args;
  result = run_process_safe(empty_args, stdout_output, stderr_output, 1024);
  EXPECT_EQ(result, -EINVAL);

  // Test timeout scenario
  std::vector<std::string> sleep_args = {"/bin/sleep", "3"};
  result = run_process_safe(sleep_args, stdout_output, stderr_output, 1024, 1);
  EXPECT_EQ(result, -ETIMEDOUT);

  // Test process that exits with non-zero status
  std::vector<std::string> fail_args = {"/bin/sh", "-c", "exit 1"};
  result = run_process_safe(fail_args, stdout_output, stderr_output, 1024);
  EXPECT_EQ(result, 1);

  // Test command that generates large stdout (should complete normally)
  std::vector<std::string> large_output_args = {"/bin/sh", "-c",
                                                "printf '%s' $(seq 1 100)"};
  result =
      run_process_safe(large_output_args, stdout_output, stderr_output, 10240);
  EXPECT_EQ(result, 0);
  EXPECT_FALSE(stdout_output.empty());

  // Test with zero timeout (should not timeout)
  std::vector<std::string> quick_args = {"/bin/echo", "quick"};
  result = run_process_safe(quick_args, stdout_output, stderr_output, 1024, 0);
  EXPECT_EQ(result, 0);
  EXPECT_EQ(stdout_output, "quick\n");

  // Test command that gets killed by signal
  // This is harder to test deterministically, but we can at least check it
  // doesn't crash
  std::vector<std::string> kill_test_args = {"/bin/sh", "-c", "kill -9 $$"};
  result = run_process_safe(kill_test_args, stdout_output, stderr_output, 1024);
  EXPECT_NE(result, 0);
}

TEST(CommonUtilsTest, parse_iso8601_time) {
  // Test valid ISO 8601 time format
  // 2023-01-01T00:00:00Z should be 1672531200 Unix timestamp
  time_t result = parse_iso8601_time("2023-01-01T00:00:00Z");
  EXPECT_EQ(result, 1672531200);

  // Test with another timestamp: 2022-12-31T23:59:59Z
  // Should be 1672531199 Unix timestamp
  result = parse_iso8601_time("2022-12-31T23:59:59Z");
  EXPECT_EQ(result, 1672531199);

  // Test Unix epoch
  result = parse_iso8601_time("1970-01-01T00:00:00Z");
  EXPECT_EQ(result, 0);

  // Test with longer string (should still work as long as prefix is valid)
  result = parse_iso8601_time("2023-01-01T00:00:00Z_EXTRA_CHARS");
  EXPECT_EQ(result, 1672531200);

  // Test with valid characters in the middle of a string (should fail)
  result = parse_iso8601_time("prefix2023-01-01T00:00:00Zsuffix");
  EXPECT_EQ(result, 0);

  // Test with only valid year part (should fail because it's incomplete)
  result = parse_iso8601_time("2023");
  EXPECT_EQ(result, 0);

  // Test with valid year-month part (should fail because it's incomplete)
  result = parse_iso8601_time("2023-01");
  EXPECT_EQ(result, 0);

  // Test with valid date part (should fail because it's incomplete)
  result = parse_iso8601_time("2023-01-01xxxxxxxxxxxxxxxxxx");
  EXPECT_EQ(result, 0);

  // Test with date and partial time (should fail because it's incomplete)
  result = parse_iso8601_time("2023-01-01T00");
  EXPECT_EQ(result, 0);

  // Test invalid input that's too short
  result = parse_iso8601_time("202");
  EXPECT_EQ(result, 0);

  // Test with invalid format
  result = parse_iso8601_time("invalid-format");
  EXPECT_EQ(result, 0);

  // Test with empty string
  result = parse_iso8601_time("");
  EXPECT_EQ(result, 0);

  // Test with malformed date
  result = parse_iso8601_time("2023-13-01T00:00:00Z");  // Invalid month
  EXPECT_EQ(result, 0);

  // Test with malformed time
  result = parse_iso8601_time("2023-01-01T25:00:00Z");  // Invalid hour
  EXPECT_EQ(result, 0);
}

TEST(CommonUtilsTest, is_valid_fd) {
  EXPECT_TRUE(is_valid_fd(0));

  int fd1 = open("/dev/null", O_RDONLY);
  ASSERT_NE(-1, fd1);
  EXPECT_TRUE(is_valid_fd(fd1));

  close(fd1);
  EXPECT_FALSE(is_valid_fd(fd1));
}

TEST(CommonUtilsTest, split_string) {
  // Test basic splitting
  auto result = split_string("a/b/c", "/");
  ASSERT_EQ(result.size(), 3ULL);
  EXPECT_EQ(result[0], "a");
  EXPECT_EQ(result[1], "b");
  EXPECT_EQ(result[2], "c");

  // Test with empty string
  result = split_string("", "/");
  ASSERT_EQ(result.size(), 1ULL);
  EXPECT_EQ(result[0], "");

  // Test with empty delimiter
  result = split_string("abc", "");
  ASSERT_EQ(result.size(), 1ULL);
  EXPECT_EQ(result[0], "abc");

  // Test with consecutive delimiters
  result = split_string("a::b::c", "::");
  ASSERT_EQ(result.size(), 3ULL);
  EXPECT_EQ(result[0], "a");
  EXPECT_EQ(result[1], "b");
  EXPECT_EQ(result[2], "c");

  // Test with trailing delimiter
  result = split_string("a/b/c/", "/");
  ASSERT_EQ(result.size(), 4ULL);
  EXPECT_EQ(result[0], "a");
  EXPECT_EQ(result[1], "b");
  EXPECT_EQ(result[2], "c");
  EXPECT_EQ(result[3], "");

  // Test with leading delimiter
  result = split_string("/a/b/c", "/");
  ASSERT_EQ(result.size(), 4ULL);
  EXPECT_EQ(result[0], "");
  EXPECT_EQ(result[1], "a");
  EXPECT_EQ(result[2], "b");
  EXPECT_EQ(result[3], "c");

  // Test with both leading and trailing delimiters
  result = split_string("/a/b/c/", "/");
  ASSERT_EQ(result.size(), 5ULL);
  EXPECT_EQ(result[0], "");
  EXPECT_EQ(result[1], "a");
  EXPECT_EQ(result[2], "b");
  EXPECT_EQ(result[3], "c");
  EXPECT_EQ(result[4], "");

  // Test with only delimiters
  result = split_string("::::", "::");
  ASSERT_EQ(result.size(), 3ULL);
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(result[i], "");
  }

  // Test with multi-character delimiter
  result = split_string("a<>b<>c", "<>");
  ASSERT_EQ(result.size(), 3ULL);
  EXPECT_EQ(result[0], "a");
  EXPECT_EQ(result[1], "b");
  EXPECT_EQ(result[2], "c");

  // Test with delimiter longer than string
  result = split_string("ab", "abcde");
  ASSERT_EQ(result.size(), 1ULL);
  EXPECT_EQ(result[0], "ab");

  // Test with complex multi-character delimiter
  result = split_string("path=>subpath=>file=>ext", "=>");
  ASSERT_EQ(result.size(), 4ULL);
  EXPECT_EQ(result[0], "path");
  EXPECT_EQ(result[1], "subpath");
  EXPECT_EQ(result[2], "file");
  EXPECT_EQ(result[3], "ext");

  // Test with overlapping potential delimiters
  result = split_string("abaabab", "ab");
  ASSERT_EQ(result.size(), 4ULL);
  EXPECT_EQ(result[0], "");
  EXPECT_EQ(result[1], "a");
  EXPECT_EQ(result[2], "");
  EXPECT_EQ(result[3], "");

  // Test with unicode characters
  result = split_string("café/世界/🌟", "/");
  ASSERT_EQ(result.size(), 3ULL);
  EXPECT_EQ(result[0], "café");
  EXPECT_EQ(result[1], "世界");
  EXPECT_EQ(result[2], "🌟");

  // Test with very long string
  std::string long_str = "token";
  for (int i = 0; i < 100; i++) {
    long_str += "|token";
  }
  result = split_string(long_str, "|");
  ASSERT_EQ(result.size(), 101ULL);
  for (int i = 0; i < 101; i++) {
    EXPECT_EQ(result[i], "token");
  }

  // Test with different newline delimiters
  result = split_string("line1\nline2\nline3", "\n");
  ASSERT_EQ(result.size(), 3ULL);
  EXPECT_EQ(result[0], "line1");
  EXPECT_EQ(result[1], "line2");
  EXPECT_EQ(result[2], "line3");

  // Test with mixed delimiters
  result = split_string("a,b;c:d", ",");
  ASSERT_EQ(result.size(), 2ULL);
  EXPECT_EQ(result[0], "a");
  EXPECT_EQ(result[1], "b;c:d");
}

TEST(CommonUtilsTest, path) {
  LOG_INFO(std::filesystem::path("a/b").lexically_relative("c").string());
}
