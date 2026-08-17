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

// JindoSDK dynamic loader: loads libjindosdk_c.so.6 at runtime via dlopen
// only when HDFS mode is actually used, avoiding atfork handler registration
// in OSS-only mode (which causes deadlock with ASAN).

#pragma once

#include <jdo_api.h>
#include <jdo_error.h>
#include <jdo_file_status.h>
#include <jdo_list_dir_result.h>
#include <jdo_lock_info.h>
#include <jdo_options.h>
#include <jdo_xattr.h>

namespace JindoSDK {

// Load libjindosdk_c.so.6 and resolve all function symbols.
// Must be called once before any jdo_* function is used.
// Returns true on success, false on failure.
bool load();

// --- Store lifecycle ---
extern JdoStore_t (*createStore)(JdoOptions_t options, const char *uri);
extern void (*destroyStore)(JdoStore_t store);
extern void (*freeStore)(JdoStore_t store);

// --- Handle context ---
extern void (*init)(JdoHandleCtx_t ctx, const char *user);
extern JdoHandleCtx_t (*createHandleCtx1)(JdoStore_t store);
extern JdoHandleCtx_t (*createHandleCtx2)(JdoStore_t store,
                                          JdoIOContext_t io_context);
extern void (*freeHandleCtx)(JdoHandleCtx_t ctx);
extern int32_t (*getHandleCtxErrorCode)(JdoHandleCtx_t ctx);
extern const char *(*getHandleCtxErrorMsg)(JdoHandleCtx_t ctx);

// --- Options ---
extern JdoOptions_t (*createOptions)();
extern void (*freeOptions)(JdoOptions_t options);
extern void (*setOption)(JdoOptions_t options, const char *key,
                         const char *val);

// --- File status ---
extern JdoFileStatus_t (*getFileStatus)(JdoHandleCtx_t ctx, const char *path,
                                        JdoOptions_t options);
extern JdoFileStatus_t (*getFileLinkStatus)(JdoHandleCtx_t ctx,
                                            const char *path,
                                            JdoOptions_t options);
extern void (*freeFileStatus)(JdoFileStatus_t fileStatus);
extern const char *(*getFileStatusPath)(JdoFileStatus_t fileStatus);
extern const char *(*getFileStatusUser)(JdoFileStatus_t fileStatus);
extern const char *(*getFileStatusGroup)(JdoFileStatus_t fileStatus);
extern int8_t (*getFileStatusType)(JdoFileStatus_t fileStatus);
extern int16_t (*getFileStatusPerm)(JdoFileStatus_t fileStatus);
extern int64_t (*getFileStatusSize)(JdoFileStatus_t fileStatus);
extern int64_t (*getFileStatusMtime)(JdoFileStatus_t fileStatus);
extern int64_t (*getFileStatusAtime)(JdoFileStatus_t fileStatus);

// --- List dir ---
extern JdoListDirResult_t (*listDir)(JdoHandleCtx_t ctx, const char *path,
                                     bool recursive, JdoOptions_t options);
extern void (*freeListDirResult)(JdoListDirResult_t listDirResult);
extern int64_t (*getListDirResultSize)(JdoListDirResult_t listDirResult);
extern bool (*isListDirResultTruncated)(JdoListDirResult_t listDirResult);
extern const char *(*getListDirResultNextMarker)(
    JdoListDirResult_t listDirResult);
extern JdoFileStatus_t (*getListDirFileStatus)(JdoListDirResult_t listDirResult,
                                               size_t index);

// --- File operations ---
extern bool (*mkdir)(JdoHandleCtx_t ctx, const char *path, bool createParent,
                     int16_t perm, JdoOptions_t options);
extern bool (*rename)(JdoHandleCtx_t ctx, const char *oldpath,
                      const char *newpath, JdoOptions_t options);
extern bool (*remove)(JdoHandleCtx_t ctx, const char *path, bool recursive,
                      JdoOptions_t options);
extern bool (*truncate)(JdoHandleCtx_t ctx, const char *path, int64_t pos,
                        JdoOptions_t options);
extern bool (*fallocate)(JdoHandleCtx_t ctx, const char *path, int64_t offset,
                         int64_t len, int32_t mode, JdoOptions_t options);

// --- Permission / ownership / times ---
extern bool (*setPermission)(JdoHandleCtx_t ctx, const char *path, int16_t perm,
                             JdoOptions_t options);
extern bool (*setOwner)(JdoHandleCtx_t ctx, const char *path, const char *user,
                        const char *group, JdoOptions_t options);
extern bool (*setTimes)(JdoHandleCtx_t ctx, const char *path, int64_t mtime,
                        int64_t atime, JdoOptions_t options);

// --- IO context (open/read/write/close) ---
extern JdoIOContext_t (*open)(JdoHandleCtx_t ctx, const char *path,
                              int32_t flags, int16_t perm,
                              JdoOptions_t options);
extern bool (*close)(JdoHandleCtx_t ctx, JdoOptions_t options);
extern void (*freeIOContext)(JdoIOContext_t io_context);
extern int64_t (*read)(JdoHandleCtx_t ctx, char *buffer, int64_t length,
                       JdoOptions_t options);
extern int64_t (*pread)(JdoHandleCtx_t ctx, char *buffer, int64_t length,
                        int64_t offset, JdoOptions_t options);
extern int64_t (*write)(JdoHandleCtx_t ctx, const char *buffer, int64_t length,
                        JdoOptions_t options);
extern int64_t (*seek)(JdoHandleCtx_t ctx, int64_t offset,
                       JdoOptions_t options);
extern int64_t (*tell)(JdoHandleCtx_t ctx, JdoOptions_t options);
extern int64_t (*getFileLength)(JdoHandleCtx_t ctx, JdoOptions_t options);
extern bool (*flush)(JdoHandleCtx_t ctx, JdoOptions_t options);

// --- Lock ---
extern JdoLockInfo_t (*createLockInfo)();
extern void (*freeLockInfo)(JdoLockInfo_t lockInfo);
extern void (*setLockInfoOffset)(JdoLockInfo_t lockInfo, int64_t offset);
extern int64_t (*getLockInfoOffset)(JdoLockInfo_t lockInfo);
extern void (*setLockInfoLength)(JdoLockInfo_t lockInfo, int64_t length);
extern int64_t (*getLockInfoLength)(JdoLockInfo_t lockInfo);
extern void (*setLockInfoType)(JdoLockInfo_t lockInfo, int16_t lockType);
extern int16_t (*getLockInfoType)(JdoLockInfo_t lockInfo);
extern void (*setLockInfoPid)(JdoLockInfo_t lockInfo, int64_t pid);
extern int64_t (*getLockInfoPid)(JdoLockInfo_t lockInfo);
extern void (*setLockInfoOwner)(JdoLockInfo_t lockInfo, uint64_t owner);
extern uint64_t (*getLockInfoOwner)(JdoLockInfo_t lockInfo);
extern bool (*setLock)(JdoHandleCtx_t ctx, const char *path,
                       JdoLockInfo_t lockInfo, JdoOptions_t options);
extern JdoLockInfo_t (*getLock)(JdoHandleCtx_t ctx, const char *path,
                                JdoLockInfo_t lockInfo, JdoOptions_t options);

// --- Symlink ---
extern bool (*createSymlink)(JdoHandleCtx_t ctx, const char *target,
                             const char *link, bool createParent,
                             JdoOptions_t options);
extern char *(*getLinkTarget)(JdoHandleCtx_t ctx, const char *path,
                              JdoOptions_t options);

// --- XAttr ---
extern JdoXAttr_t (*createXAttr)();
extern void (*freeXAttr)(JdoXAttr_t xAttr);
extern void (*setXAttrNamespace)(JdoXAttr_t xAttr, int ns);
extern int (*getXAttrNamespace)(JdoXAttr_t xAttr);
extern void (*setXAttrName)(JdoXAttr_t xAttr, const char *name);
extern char *(*getXAttrName)(JdoXAttr_t xAttr);
extern void (*setXAttrValue)(JdoXAttr_t xAttr, const char *val);
extern char *(*getXAttrValue)(JdoXAttr_t xAttr);
extern JdoXAttrList_t (*getXAttrs)(JdoHandleCtx_t ctx, const char *path,
                                   JdoOptions_t options);
extern void (*freeXAttrList)(JdoXAttrList_t xAttrList);
extern int64_t (*getXAttrListSize)(JdoXAttrList_t xAttrList);
extern JdoXAttr_t (*getXAttrsListIterator)(JdoXAttrList_t xAttrList,
                                           int64_t index);
extern bool (*setXAttr)(JdoHandleCtx_t ctx, const char *path, JdoXAttr_t xAttr,
                        int32_t flag, JdoOptions_t options);
extern bool (*removeXAttr)(JdoHandleCtx_t ctx, const char *path,
                           JdoXAttr_t xAttr, JdoOptions_t options);

}  // namespace JindoSDK
