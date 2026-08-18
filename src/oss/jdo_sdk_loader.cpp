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

#include "jdo_sdk_loader.h"

#include <dlfcn.h>

#include <mutex>

#include "common/logger.h"

namespace JindoSDK {

static void *g_sdk_handle = nullptr;

// --- Function pointer definitions (all initialized to nullptr) ---

// Store lifecycle
JdoStore_t (*createStore)(JdoOptions_t, const char *) = nullptr;
void (*destroyStore)(JdoStore_t) = nullptr;
void (*freeStore)(JdoStore_t) = nullptr;

// Handle context
void (*init)(JdoHandleCtx_t, const char *) = nullptr;
JdoHandleCtx_t (*createHandleCtx1)(JdoStore_t) = nullptr;
JdoHandleCtx_t (*createHandleCtx2)(JdoStore_t, JdoIOContext_t) = nullptr;
void (*freeHandleCtx)(JdoHandleCtx_t) = nullptr;
int32_t (*getHandleCtxErrorCode)(JdoHandleCtx_t) = nullptr;
const char *(*getHandleCtxErrorMsg)(JdoHandleCtx_t) = nullptr;

// Options
JdoOptions_t (*createOptions)() = nullptr;
void (*freeOptions)(JdoOptions_t) = nullptr;
void (*setOption)(JdoOptions_t, const char *, const char *) = nullptr;

// File status
JdoFileStatus_t (*getFileStatus)(JdoHandleCtx_t, const char *,
                                 JdoOptions_t) = nullptr;
JdoFileStatus_t (*getFileLinkStatus)(JdoHandleCtx_t, const char *,
                                     JdoOptions_t) = nullptr;
void (*freeFileStatus)(JdoFileStatus_t) = nullptr;
const char *(*getFileStatusPath)(JdoFileStatus_t) = nullptr;
const char *(*getFileStatusUser)(JdoFileStatus_t) = nullptr;
const char *(*getFileStatusGroup)(JdoFileStatus_t) = nullptr;
int8_t (*getFileStatusType)(JdoFileStatus_t) = nullptr;
int16_t (*getFileStatusPerm)(JdoFileStatus_t) = nullptr;
int64_t (*getFileStatusSize)(JdoFileStatus_t) = nullptr;
int64_t (*getFileStatusMtime)(JdoFileStatus_t) = nullptr;
int64_t (*getFileStatusAtime)(JdoFileStatus_t) = nullptr;

// List dir
JdoListDirResult_t (*listDir)(JdoHandleCtx_t, const char *, bool,
                              JdoOptions_t) = nullptr;
void (*freeListDirResult)(JdoListDirResult_t) = nullptr;
int64_t (*getListDirResultSize)(JdoListDirResult_t) = nullptr;
bool (*isListDirResultTruncated)(JdoListDirResult_t) = nullptr;
const char *(*getListDirResultNextMarker)(JdoListDirResult_t) = nullptr;
JdoFileStatus_t (*getListDirFileStatus)(JdoListDirResult_t, size_t) = nullptr;

// File operations
bool (*mkdir)(JdoHandleCtx_t, const char *, bool, int16_t,
              JdoOptions_t) = nullptr;
bool (*rename)(JdoHandleCtx_t, const char *, const char *,
               JdoOptions_t) = nullptr;
bool (*remove)(JdoHandleCtx_t, const char *, bool, JdoOptions_t) = nullptr;
bool (*truncate)(JdoHandleCtx_t, const char *, int64_t, JdoOptions_t) = nullptr;
bool (*fallocate)(JdoHandleCtx_t, const char *, int64_t, int64_t, int32_t,
                  JdoOptions_t) = nullptr;

// Permission / ownership / times
bool (*setPermission)(JdoHandleCtx_t, const char *, int16_t,
                      JdoOptions_t) = nullptr;
bool (*setOwner)(JdoHandleCtx_t, const char *, const char *, const char *,
                 JdoOptions_t) = nullptr;
bool (*setTimes)(JdoHandleCtx_t, const char *, int64_t, int64_t,
                 JdoOptions_t) = nullptr;

// IO context
JdoIOContext_t (*open)(JdoHandleCtx_t, const char *, int32_t, int16_t,
                       JdoOptions_t) = nullptr;
bool (*close)(JdoHandleCtx_t, JdoOptions_t) = nullptr;
void (*freeIOContext)(JdoIOContext_t) = nullptr;
int64_t (*read)(JdoHandleCtx_t, char *, int64_t, JdoOptions_t) = nullptr;
int64_t (*pread)(JdoHandleCtx_t, char *, int64_t, int64_t,
                 JdoOptions_t) = nullptr;
int64_t (*write)(JdoHandleCtx_t, const char *, int64_t, JdoOptions_t) = nullptr;
int64_t (*seek)(JdoHandleCtx_t, int64_t, JdoOptions_t) = nullptr;
int64_t (*tell)(JdoHandleCtx_t, JdoOptions_t) = nullptr;
int64_t (*getFileLength)(JdoHandleCtx_t, JdoOptions_t) = nullptr;
bool (*flush)(JdoHandleCtx_t, JdoOptions_t) = nullptr;

// Lock
JdoLockInfo_t (*createLockInfo)() = nullptr;
void (*freeLockInfo)(JdoLockInfo_t) = nullptr;
void (*setLockInfoOffset)(JdoLockInfo_t, int64_t) = nullptr;
int64_t (*getLockInfoOffset)(JdoLockInfo_t) = nullptr;
void (*setLockInfoLength)(JdoLockInfo_t, int64_t) = nullptr;
int64_t (*getLockInfoLength)(JdoLockInfo_t) = nullptr;
void (*setLockInfoType)(JdoLockInfo_t, int16_t) = nullptr;
int16_t (*getLockInfoType)(JdoLockInfo_t) = nullptr;
void (*setLockInfoPid)(JdoLockInfo_t, int64_t) = nullptr;
int64_t (*getLockInfoPid)(JdoLockInfo_t) = nullptr;
void (*setLockInfoOwner)(JdoLockInfo_t, uint64_t) = nullptr;
uint64_t (*getLockInfoOwner)(JdoLockInfo_t) = nullptr;
bool (*setLock)(JdoHandleCtx_t, const char *, JdoLockInfo_t,
                JdoOptions_t) = nullptr;
JdoLockInfo_t (*getLock)(JdoHandleCtx_t, const char *, JdoLockInfo_t,
                         JdoOptions_t) = nullptr;

// Symlink
bool (*createSymlink)(JdoHandleCtx_t, const char *, const char *, bool,
                      JdoOptions_t) = nullptr;
char *(*getLinkTarget)(JdoHandleCtx_t, const char *, JdoOptions_t) = nullptr;

// XAttr
JdoXAttr_t (*createXAttr)() = nullptr;
void (*freeXAttr)(JdoXAttr_t) = nullptr;
void (*setXAttrNamespace)(JdoXAttr_t, int) = nullptr;
int (*getXAttrNamespace)(JdoXAttr_t) = nullptr;
void (*setXAttrName)(JdoXAttr_t, const char *) = nullptr;
char *(*getXAttrName)(JdoXAttr_t) = nullptr;
void (*setXAttrValue)(JdoXAttr_t, const char *) = nullptr;
char *(*getXAttrValue)(JdoXAttr_t) = nullptr;
JdoXAttrList_t (*getXAttrs)(JdoHandleCtx_t, const char *,
                            JdoOptions_t) = nullptr;
void (*freeXAttrList)(JdoXAttrList_t) = nullptr;
int64_t (*getXAttrListSize)(JdoXAttrList_t) = nullptr;
JdoXAttr_t (*getXAttrsListIterator)(JdoXAttrList_t, int64_t) = nullptr;
bool (*setXAttr)(JdoHandleCtx_t, const char *, JdoXAttr_t, int32_t,
                 JdoOptions_t) = nullptr;
bool (*removeXAttr)(JdoHandleCtx_t, const char *, JdoXAttr_t,
                    JdoOptions_t) = nullptr;

// --- Loader ---

#define LOAD_SYM(name)                                             \
  do {                                                             \
    name = reinterpret_cast<decltype(name)>(dlsym(handle, #name)); \
    if (!name) {                                                   \
      LOG_ERROR("dlsym failed for `", #name);                      \
      return false;                                                \
    }                                                              \
  } while (0)

// For functions whose C name has jdo_ prefix but our pointer drops it.
#define LOAD_JDO(ptr_name, c_name)                                           \
  do {                                                                       \
    ptr_name = reinterpret_cast<decltype(ptr_name)>(dlsym(handle, #c_name)); \
    if (!ptr_name) {                                                         \
      LOG_ERROR("dlsym failed for `", #c_name);                              \
      result = false;                                                        \
      return;                                                                \
    }                                                                        \
  } while (0)

bool load() {
  static std::once_flag once;
  static bool result = false;

  std::call_once(once, []() {
    void *handle = dlopen("libjindosdk_c.so.6", RTLD_NOW | RTLD_GLOBAL);
    if (!handle) {
      LOG_ERROR("dlopen libjindosdk_c.so.6 failed: `", dlerror());
      result = false;
      return;
    }
    g_sdk_handle = handle;

    // Store lifecycle
    LOAD_JDO(createStore, jdo_createStore);
    LOAD_JDO(destroyStore, jdo_destroyStore);
    LOAD_JDO(freeStore, jdo_freeStore);

    // Handle context
    LOAD_JDO(init, jdo_init);
    LOAD_JDO(createHandleCtx1, jdo_createHandleCtx1);
    LOAD_JDO(createHandleCtx2, jdo_createHandleCtx2);
    LOAD_JDO(freeHandleCtx, jdo_freeHandleCtx);
    LOAD_JDO(getHandleCtxErrorCode, jdo_getHandleCtxErrorCode);
    LOAD_JDO(getHandleCtxErrorMsg, jdo_getHandleCtxErrorMsg);

    // Options
    LOAD_JDO(createOptions, jdo_createOptions);
    LOAD_JDO(freeOptions, jdo_freeOptions);
    LOAD_JDO(setOption, jdo_setOption);

    // File status
    LOAD_JDO(getFileStatus, jdo_getFileStatus);
    LOAD_JDO(getFileLinkStatus, jdo_getFileLinkStatus);
    LOAD_JDO(freeFileStatus, jdo_freeFileStatus);
    LOAD_JDO(getFileStatusPath, jdo_getFileStatusPath);
    LOAD_JDO(getFileStatusUser, jdo_getFileStatusUser);
    LOAD_JDO(getFileStatusGroup, jdo_getFileStatusGroup);
    LOAD_JDO(getFileStatusType, jdo_getFileStatusType);
    LOAD_JDO(getFileStatusPerm, jdo_getFileStatusPerm);
    LOAD_JDO(getFileStatusSize, jdo_getFileStatusSize);
    LOAD_JDO(getFileStatusMtime, jdo_getFileStatusMtime);
    LOAD_JDO(getFileStatusAtime, jdo_getFileStatusAtime);

    // List dir
    LOAD_JDO(listDir, jdo_listDir);
    LOAD_JDO(freeListDirResult, jdo_freeListDirResult);
    LOAD_JDO(getListDirResultSize, jdo_getListDirResultSize);
    LOAD_JDO(isListDirResultTruncated, jdo_isListDirResultTruncated);
    LOAD_JDO(getListDirResultNextMarker, jdo_getListDirResultNextMarker);
    LOAD_JDO(getListDirFileStatus, jdo_getListDirFileStatus);

    // File operations
    LOAD_JDO(mkdir, jdo_mkdir);
    LOAD_JDO(rename, jdo_rename);
    LOAD_JDO(remove, jdo_remove);
    LOAD_JDO(truncate, jdo_truncate);
    LOAD_JDO(fallocate, jdo_fallocate);

    // Permission / ownership / times
    LOAD_JDO(setPermission, jdo_setPermission);
    LOAD_JDO(setOwner, jdo_setOwner);
    LOAD_JDO(setTimes, jdo_setTimes);

    // IO context
    LOAD_JDO(open, jdo_open);
    LOAD_JDO(close, jdo_close);
    LOAD_JDO(freeIOContext, jdo_freeIOContext);
    LOAD_JDO(read, jdo_read);
    LOAD_JDO(pread, jdo_pread);
    LOAD_JDO(write, jdo_write);
    LOAD_JDO(seek, jdo_seek);
    LOAD_JDO(tell, jdo_tell);
    LOAD_JDO(getFileLength, jdo_getFileLength);
    LOAD_JDO(flush, jdo_flush);

    // Lock
    LOAD_JDO(createLockInfo, jdo_createLockInfo);
    LOAD_JDO(freeLockInfo, jdo_freeLockInfo);
    LOAD_JDO(setLockInfoOffset, jdo_setLockInfoOffset);
    LOAD_JDO(getLockInfoOffset, jdo_getLockInfoOffset);
    LOAD_JDO(setLockInfoLength, jdo_setLockInfoLength);
    LOAD_JDO(getLockInfoLength, jdo_getLockInfoLength);
    LOAD_JDO(setLockInfoType, jdo_setLockInfoType);
    LOAD_JDO(getLockInfoType, jdo_getLockInfoType);
    LOAD_JDO(setLockInfoPid, jdo_setLockInfoPid);
    LOAD_JDO(getLockInfoPid, jdo_getLockInfoPid);
    LOAD_JDO(setLockInfoOwner, jdo_setLockInfoOwner);
    LOAD_JDO(getLockInfoOwner, jdo_getLockInfoOwner);
    LOAD_JDO(setLock, jdo_setLock);
    LOAD_JDO(getLock, jdo_getLock);

    // Symlink
    LOAD_JDO(createSymlink, jdo_createSymlink);
    LOAD_JDO(getLinkTarget, jdo_getLinkTarget);

    // XAttr
    LOAD_JDO(createXAttr, jdo_createXAttr);
    LOAD_JDO(freeXAttr, jdo_freeXAttr);
    LOAD_JDO(setXAttrNamespace, jdo_setXAttrNamespace);
    LOAD_JDO(getXAttrNamespace, jdo_getXAttrNamespace);
    LOAD_JDO(setXAttrName, jdo_setXAttrName);
    LOAD_JDO(getXAttrName, jdo_getXAttrName);
    LOAD_JDO(setXAttrValue, jdo_setXAttrValue);
    LOAD_JDO(getXAttrValue, jdo_getXAttrValue);
    LOAD_JDO(getXAttrs, jdo_getXAttrs);
    LOAD_JDO(freeXAttrList, jdo_freeXAttrList);
    LOAD_JDO(getXAttrListSize, jdo_getXAttrListSize);
    LOAD_JDO(getXAttrsListIterator, jdo_getXAttrsListIterator);
    LOAD_JDO(setXAttr, jdo_setXAttr);
    LOAD_JDO(removeXAttr, jdo_removeXAttr);

    result = true;
    LOG_INFO("JindoSDK loaded successfully via dlopen");
  });

  return result;
}

#undef LOAD_SYM
#undef LOAD_JDO

}  // namespace JindoSDK
