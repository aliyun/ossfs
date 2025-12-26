/*
 * ossfs -  FUSE-based file system backed by Alibaba Cloud OSS
 *
 * Copyright(C) 2007 Randy Rizun <rrizun@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef S3FS_CRED_H_
#define S3FS_CRED_H_

#include "autolock.h"
#include "s3fs_extcred.h"

//----------------------------------------------
// Typedefs
//----------------------------------------------
typedef std::map<std::string, std::string> ramcredmap_t;

//------------------------------------------------
// class S3fsCred
//------------------------------------------------
// This is a class for operating and managing Credentials(accesskey,
// secret key, tokens, etc.) used by S3fs.
// Operations related to Credentials are aggregated in this class.
//
// cppcheck-suppress ctuOneDefinitionRuleViolation       ; for stub in test_curl_util.cpp
class S3fsCred
{
    private:
        static const char*  ALLBUCKET_FIELDS_TYPE;      // special key for mapping(This name is absolutely not used as a bucket name)
        static const char*  KEYVAL_FIELDS_TYPE;         // special key for mapping(This name is absolutely not used as a bucket name)
        static const char*  KEY_ACCESSKEYID;
        static const char*  KEY_SECRETKEY;

        static const int    RAM_EXPIRE_MERGIN;
        static const char*  ECS_RAM_ENV_VAR;
        static const char*  RAMCRED_ACCESSKEYID;
        static const char*  RAMCRED_SECRETACCESSKEY;
        static const char*  RAMCRED_ACCESSKEYSECRET;
        static const char*  RAMCRED_ROLEARN;

        static std::string  bucket_name;

        pthread_mutex_t     token_lock;
        bool                is_lock_init;

        std::string         passwd_file;

        bool                load_ramrole;        // true: LoadIAMRoleFromMetaData

        std::string         AccessKeyId;         // Protect exclusively
        std::string         AccessKeySecret;     // Protect exclusively
        std::string         SecurityToken;         // Protect exclusively
        time_t              SecurityTokenExpire;   // Protect exclusively

        bool                is_use_session_token;

        std::string         RAM_cred_url;
        size_t              RAM_field_count;
        std::string         RAM_token_field;
        std::string         RAM_expiry_field;
        std::string         RAM_role;               // Protect exclusively

        bool                imds_v2 = true;

        bool                set_builtin_cred_opts;  // true if options other than "credlib" is set
        std::string         credlib;                // credlib(name or path)
        std::string         credlib_opts;           // options for credlib

        void*                    hExtCredLib;
        fp_VersionS3fsCredential pFuncCredVersion;
        fp_InitS3fsCredential    pFuncCredInit;
        fp_FreeS3fsCredential    pFuncCredFree;
        fp_UpdateS3fsCredential  pFuncCredUpdate;

        std::string         credential_process;
    public:
        static std::string       RAMv2_token_hdr;

    private:
        static bool ParseRAMRoleFromMetaDataResponse(const char* response, std::string& rolename);

        bool SetPasswdFile(const char* file);
        bool IsSetPasswdFile() const;
        bool SetRamRoleMetadataType(bool flag);

        bool SetAccessKey(const char* AccessKeyId, const char* SecretAccessKey, AutoLock::Type type);
        bool SetAccessKeyWithSessionToken(const char* AccessKeyId, const char* SecretAccessKey, const char * SessionToken, AutoLock::Type type);
        bool IsSetAccessKeys(AutoLock::Type type);

        bool SetIsUseSessionToken(bool flag);

        bool SetRAMRole(const char* role, AutoLock::Type type);
        std::string GetRAMRole(AutoLock::Type type);
        bool IsSetRAMRole(AutoLock::Type type);
        size_t SetRAMFieldCount(size_t field_count);
        std::string SetRAMCredentialsURL(const char* url);
        std::string SetIAMTokenField(const char* token_field);
        std::string SetIAMExpiryField(const char* expiry_field);

        bool IsReadablePasswdFile();
        bool CheckPasswdFilePerms();
        bool ParsePasswdFile(bucketkvmap_t& resmap);
        bool ReadPasswdFile(AutoLock::Type type);

        int CheckCredentialOssFormat(const kvmap_t& kvmap, std::string& access_key_id, std::string& secret_access_key);

        bool InitialCredentials();
        bool ParseRAMCredentialResponse(const char* response, ramcredmap_t& keyval);

        bool GetRAMCredentialsURL(std::string& url, bool check_ram_role, AutoLock::Type type);
        bool LoadRAMCredentials(AutoLock::Type type);
        bool SetRAMCredentials(const char* response,  AutoLock::Type type, bool check_field_cnt = true);
        bool SetRAMRoleFromMetaData(const char* response, AutoLock::Type type);
        bool GetTokenIMDSV2(std::string &token);

        bool SetExtCredLib(const char* arg);
        bool IsSetExtCredLib() const;
        bool SetExtCredLibOpts(const char* args);
        bool IsSetExtCredLibOpts() const;

        bool InitExtCredLib();
        bool LoadExtCredLib();
        bool UnloadExtCredLib();
        bool UpdateExtCredentials(AutoLock::Type type);

        bool CheckForbiddenBucketParams();

        bool SetCredProcess(const char* arg);
        bool IsSetCredProcess() const;
        bool UpdateCredFromProc(AutoLock::Type type);

    public:
        static bool SetBucket(const char* bucket);
        static const std::string& GetBucket();

        S3fsCred();
        ~S3fsCred();

        bool IsIBMIAMAuth() const { return false; }

        bool LoadIAMRoleFromMetaData();

        bool CheckIAMCredentialUpdate(std::string* access_key_id = NULL, std::string* secret_access_key = NULL, std::string* access_token = NULL);
        const char* GetCredFuncVersion(bool detail) const;

        int DetectParam(const char* arg);
        bool CheckAllParams();
};

#endif // S3FS_CRED_H_

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
