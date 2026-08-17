#pragma once

#include <photon/common/checksum/crc64ecma.h>

#include <cstdint>

namespace OssFileSystem {

// Combine two CRC64-ECMA values: given crc1 = CRC(A), crc2 = CRC(B), and
// len2 = length(B), returns CRC(A || B) without access to the original data.
// Uses GF(2) matrix exponentiation (same algorithm as zlib's crc32_combine).
inline uint64_t crc64ecma_combine(uint64_t crc1, uint64_t crc2, uint64_t len2) {
  if (len2 == 0) return crc1;

  // GF(2) matrix representation of "append zero bits" to a CRC.
  uint64_t even[64], odd[64];

  // odd = matrix representing "append 1 zero bit"
  odd[0] = 0xc96c5795d7870f42ULL;  // CRC64-ECMA polynomial (reflected)
  uint64_t row = 1;
  for (int i = 1; i < 64; i++) {
    odd[i] = row;
    row <<= 1;
  }

  // even = odd^2 (append 2 zero bits)
  auto gf2_matrix_square = [](uint64_t *square, const uint64_t *mat) {
    for (int i = 0; i < 64; i++) {
      uint64_t v = mat[i], s = 0;
      for (int j = 0; v; j++, v >>= 1)
        if (v & 1) s ^= mat[j];
      square[i] = s;
    }
  };
  gf2_matrix_square(even, odd);
  // odd = even^2 (append 4 zero bits)
  gf2_matrix_square(odd, even);

  // Apply len2 bytes of zeros. After initial setup odd = 4 zero bits;
  // the first loop square produces even = 8 zero bits = 1 byte, so the
  // loop iterates over bits of len2 (in bytes), matching zlib's approach.
  auto gf2_matrix_times = [](const uint64_t *mat, uint64_t vec) -> uint64_t {
    uint64_t s = 0;
    for (int i = 0; vec; i++, vec >>= 1)
      if (vec & 1) s ^= mat[i];
    return s;
  };

  do {
    gf2_matrix_square(even, odd);
    if (len2 & 1) crc1 = gf2_matrix_times(even, crc1);
    len2 >>= 1;
    if (len2 == 0) break;
    gf2_matrix_square(odd, even);
    if (len2 & 1) crc1 = gf2_matrix_times(odd, crc1);
    len2 >>= 1;
  } while (len2 != 0);

  return crc1 ^ crc2;
}

}  // namespace OssFileSystem
