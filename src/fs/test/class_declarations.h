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

class Ossfs2BasicTest;
class Ossfs2TestSuite;
class Ossfs2ReadWriteTest;
class Ossfs2CRC64Test;
class Ossfs2RenameTest;
class Ossfs2AppendableObjectTest;
class Ossfs2ReaddirTest;
class Ossfs2InodeRefTest;
class Ossfs2InodeTest;
class Ossfs2StagedInodeCacheTest;
class Ossfs2OpenCloseTest;
class Ossfs2CreateUnlinkTest;
class Ossfs2NegativeCacheTest;
class Ossfs2FilenameTest;
class Ossfs2MetricsTest;
class Ossfs2CredentialsTest;
class Ossfs2SymlinkTest;

#define DECLARE_TEST_FRIENDS_CLASSES         \
  friend class ::Ossfs2BasicTest;            \
  friend class ::Ossfs2TestSuite;            \
  friend class ::Ossfs2ReadWriteTest;        \
  friend class ::Ossfs2CRC64Test;            \
  friend class ::Ossfs2RenameTest;           \
  friend class ::Ossfs2AppendableObjectTest; \
  friend class ::Ossfs2ReaddirTest;          \
  friend class ::Ossfs2InodeRefTest;         \
  friend class ::Ossfs2InodeTest;            \
  friend class ::Ossfs2StagedInodeCacheTest; \
  friend class ::Ossfs2OpenCloseTest;        \
  friend class ::Ossfs2CreateUnlinkTest;     \
  friend class ::Ossfs2NegativeCacheTest;    \
  friend class ::Ossfs2FilenameTest;         \
  friend class ::Ossfs2MetricsTest;          \
  friend class ::Ossfs2CredentialsTest;      \
  friend class ::Ossfs2SymlinkTest;
