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

#pragma once

#define E_NO_DIRTY_DATA 801
#define E_CONTINUE_LOOKUP 802
#define E_READ_PATH_NEEDED 803
#define E_WRITE_PATH_NEEDED 804
#define E_LOOKUP_FROM_STAGED_CACHE 805
#define E_CONTINUE_READ 806
#define E_CONTINUE_PIN 807
#define E_DISK_CACHE_COLLISION 808
// Verified refill GET found that the anchored path no longer exists.
#define E_REFILL_PATH_ENOENT 809
// Verified refill GET found an ETag mismatch against the anchored version.
#define E_REFILL_ETAG_MISMATCH 810

static inline bool is_refill_verify_error(ssize_t r) {
  return r == -E_REFILL_PATH_ENOENT || r == -E_REFILL_ETAG_MISMATCH;
}
