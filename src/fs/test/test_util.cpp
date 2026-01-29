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

#include "test_util.h"

#include <fstream>
#include <sstream>

#define ONCE(init)                \
  do {                            \
    static volatile int once = 1; \
    if (once) {                   \
      if (once++ == 1) {          \
        init();                   \
        once = 0;                 \
      } else                      \
        while (once)              \
          ;                       \
    }                             \
  } while (0)

#define POLY UINT64_C(0xc96c5795d7870f42)
uint64_t crc64_table[8][256];

/* Fill in the CRC-64 constants table. */
void crc64_init(uint64_t table[][256]) {
  unsigned n, k;
  uint64_t crc;

  /* generate CRC-64's for all single byte sequences */
  for (n = 0; n < 256; n++) {
    crc = n;
    for (k = 0; k < 8; k++) crc = crc & 1 ? POLY ^ (crc >> 1) : crc >> 1;
    table[0][n] = crc;
  }

  /* generate CRC-64's for those followed by 1 to 7 zeros */
  for (n = 0; n < 256; n++) {
    crc = table[0][n];
    for (k = 1; k < 8; k++) {
      crc = table[0][crc & 0xff] ^ (crc >> 8);
      table[k][n] = crc;
    }
  }
}

/* This function is called once to initialize the CRC-64 table for use on a
   little-endian architecture. */
void crc64_little_init(void) {
  crc64_init(crc64_table);
}

/* Reverse the bytes in a 64-bit word. */
uint64_t rev8(uint64_t a) {
  uint64_t m;

  m = UINT64_C(0xff00ff00ff00ff);
  a = ((a >> 8) & m) | (a & m) << 8;
  m = UINT64_C(0xffff0000ffff);
  a = ((a >> 16) & m) | (a & m) << 16;
  return a >> 32 | a << 32;
}

/* This function is called once to initialize the CRC-64 table for use on a
   big-endian architecture. */
void crc64_big_init(void) {
  unsigned k, n;

  crc64_init(crc64_table);
  for (k = 0; k < 8; k++)
    for (n = 0; n < 256; n++) crc64_table[k][n] = rev8(crc64_table[k][n]);
}

/* Calculate a CRC-64 eight bytes at a time on a little-endian architecture. */
uint64_t crc64_little(uint64_t crc, void *buf, size_t len) {
  unsigned char *next = (unsigned char *)buf;

  ONCE(crc64_little_init);
  crc = ~crc;
  while (len && ((uintptr_t)next & 7) != 0) {
    crc = crc64_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
    len--;
  }
  while (len >= 8) {
    crc ^= *(uint64_t *)next;
    crc = crc64_table[7][crc & 0xff] ^ crc64_table[6][(crc >> 8) & 0xff] ^
          crc64_table[5][(crc >> 16) & 0xff] ^
          crc64_table[4][(crc >> 24) & 0xff] ^
          crc64_table[3][(crc >> 32) & 0xff] ^
          crc64_table[2][(crc >> 40) & 0xff] ^
          crc64_table[1][(crc >> 48) & 0xff] ^ crc64_table[0][crc >> 56];
    next += 8;
    len -= 8;
  }
  while (len) {
    crc = crc64_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
    len--;
  }
  return ~crc;
}

/* Calculate a CRC-64 eight bytes at a time on a big-endian architecture. */
uint64_t crc64_big(uint64_t crc, void *buf, size_t len) {
  unsigned char *next = (unsigned char *)buf;

  ONCE(crc64_big_init);
  crc = ~rev8(crc);
  while (len && ((uintptr_t)next & 7) != 0) {
    crc = crc64_table[0][(crc >> 56) ^ *next++] ^ (crc << 8);
    len--;
  }
  while (len >= 8) {
    crc ^= *(uint64_t *)next;
    crc = crc64_table[0][crc & 0xff] ^ crc64_table[1][(crc >> 8) & 0xff] ^
          crc64_table[2][(crc >> 16) & 0xff] ^
          crc64_table[3][(crc >> 24) & 0xff] ^
          crc64_table[4][(crc >> 32) & 0xff] ^
          crc64_table[5][(crc >> 40) & 0xff] ^
          crc64_table[6][(crc >> 48) & 0xff] ^ crc64_table[7][crc >> 56];
    next += 8;
    len -= 8;
  }
  while (len) {
    crc = crc64_table[0][(crc >> 56) ^ *next++] ^ (crc << 8);
    len--;
  }
  return ~rev8(crc);
}

uint64_t cal_crc64(uint64_t crc, void *buf, size_t len) {
  uint64_t n = 1;
  return *(char *)&n ? crc64_little(crc, buf, len) : crc64_big(crc, buf, len);
}

uint64_t cal_crc64(uint64_t crc, void *buf, size_t len, bool little) {
  return little ? crc64_little(crc, buf, len) : crc64_big(crc, buf, len);
}

std::string read_file(const std::string &filename) {
  std::ifstream file(filename, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Cannot open file: " + filename);
  }
  return std::string(std::istreambuf_iterator<char>(file),
                     std::istreambuf_iterator<char>());
}
