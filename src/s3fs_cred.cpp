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

#include <unistd.h>
#include <pwd.h>
#include <sys/types.h>
#include <dlfcn.h>
#include <fstream>
#include <sstream>

#include "common.h"
#include "s3fs.h"
#include "s3fs_cred.h"
#include "curl.h"
#include "string_util.h"
#include "metaheader.h"

//-------------------------------------------------------------------
// Symbols
//-------------------------------------------------------------------
#define DEFAULT_PASSWD_FILE    "/etc/passwd-ossfs"

#define DEFAULT_AWS_PROFILE_NAME    "default"

//-------------------------------------------------------------------
// External Credential dummy function
//-------------------------------------------------------------------
// [NOTE]
// This function expects the following values:
//
// detail=false   ex. "Custom AWS Credential Library - v1.0.0"
// detail=true    ex. "Custom AWS Credential Library - v1.0.0
//                     s3fs-fuse credential I/F library for S3 compatible strage X.
//                     Copyright(C) 2022 Foo"
//
const char* VersionS3fsCredential(bool detail)
{
    static const char version[]        = "built-in";
    static const char detail_version[] = 
    "ossfs built-in Credential I/F Function\n"
    "Copyright(C) 2010 ossfs\n";

    if(detail){
        S3FS_PRN_CRIT("Check why built-in function was called, the external credential library must have VersionS3fsCredential function.");
        return detail_version;
    }else{
        return version;
    }
}

bool InitS3fsCredential(const char* popts, char** pperrstr)
{
    if(popts && 0 < strlen(popts)){
        S3FS_PRN_WARN("The external credential library does not have InitS3fsCredential function, but credlib_opts value is not empty(%s)", popts);
    }
    if(pperrstr){
        *pperrstr = strdup("The external credential library does not have InitS3fsCredential function, so built-in function was called.");
    }else{
        S3FS_PRN_INFO("The external credential library does not have InitS3fsCredential function, so built-in function was called.");
    }
    return true;
}

bool FreeS3fsCredential(char** pperrstr)
{
    if(pperrstr){
        *pperrstr = strdup("The external credential library does not have FreeS3fsCredential function, so built-in function was called.");
    }else{
        S3FS_PRN_INFO("The external credential library does not have FreeS3fsCredential function, so built-in function was called.");
    }
    return true;
}

bool UpdateS3fsCredential(char** ppaccess_key_id, char** ppserect_access_key, char** ppaccess_token, long long* ptoken_expire, char** pperrstr)
{
    S3FS_PRN_INFO("Parameters : ppaccess_key_id=%p, ppserect_access_key=%p, ppaccess_token=%p, ptoken_expire=%p", ppaccess_key_id, ppserect_access_key, ppaccess_token, ptoken_expire);

    if(pperrstr){
        *pperrstr = strdup("Check why built-in function was called, the external credential library must have UpdateS3fsCredential function.");
    }else{
        S3FS_PRN_CRIT("Check why built-in function was called, the external credential library must have UpdateS3fsCredential function.");
    }

    if(ppaccess_key_id){
        *ppaccess_key_id = NULL;
    }
    if(ppserect_access_key){
        *ppserect_access_key = NULL;
    }
    if(ppaccess_token){
        *ppaccess_token = NULL;
    }
    return false;   // always false
}
//-------------------------------------------------------------------
// Class Variables
//-------------------------------------------------------------------
const char* S3fsCred::ALLBUCKET_FIELDS_TYPE     = "";
const char*	S3fsCred::KEYVAL_FIELDS_TYPE        = "\t";
const char* S3fsCred::KEY_ACCESSKEYID           = "OSSAccessKeyId";
const char* S3fsCred::KEY_SECRETKEY             = "OSSSecretKey";

const int   S3fsCred::RAM_EXPIRE_MERGIN         = 20 * 60;              // update timing
const char* S3fsCred::ECS_RAM_ENV_VAR           = "OSS_CONTAINER_CREDENTIALS_RELATIVE_URI";
const char* S3fsCred::RAMCRED_ACCESSKEYID       = "AccessKeyId";
const char* S3fsCred::RAMCRED_SECRETACCESSKEY   = "SecretAccessKey";
const char* S3fsCred::RAMCRED_ACCESSKEYSECRET   = "AccessKeySecret";
const char* S3fsCred::RAMCRED_ROLEARN           = "RoleArn";

std::string S3fsCred::bucket_name;

//-------------------------------------------------------------------
// Class Methods 
//-------------------------------------------------------------------
bool S3fsCred::SetBucket(const char* bucket)
{
    if(!bucket || strlen(bucket) == 0){
        return false;
    }
    S3fsCred::bucket_name = bucket;
    return true;
}

const std::string& S3fsCred::GetBucket()
{
	return S3fsCred::bucket_name;
}

bool S3fsCred::ParseRAMRoleFromMetaDataResponse(const char* response, std::string& rolename)
{
    if(!response){
        return false;
    }
    // [NOTE]
    // expected following strings.
    // 
    // myrolename
    //
    std::istringstream ssrole(response);
    std::string        oneline;
    if (getline(ssrole, oneline, '\n')){
        rolename = oneline;
        return !rolename.empty();
    }
    return false;
}

//-------------------------------------------------------------------
// Methods : Constructor / Destructor
//-------------------------------------------------------------------
S3fsCred::S3fsCred() :
    is_lock_init(false),
    passwd_file(""),
    load_ramrole(false),
    AccessKeyId(""),
    AccessKeySecret(""),
    SecurityToken(""),
    SecurityTokenExpire(0),
    is_use_session_token(false),
    RAM_cred_url("http://100.100.100.200/latest/meta-data/ram/security-credentials/"),
    RAM_field_count(4),
    RAM_token_field("Token"),
    RAM_expiry_field("Expiration"),
    RAM_role(""),
    set_builtin_cred_opts(false),
    hExtCredLib(NULL),
    pFuncCredVersion(VersionS3fsCredential),
    pFuncCredInit(InitS3fsCredential),
    pFuncCredFree(FreeS3fsCredential),
    pFuncCredUpdate(UpdateS3fsCredential)
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
#if S3FS_PTHREAD_ERRORCHECK
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#endif
    int result;
    if(0 != (result = pthread_mutex_init(&token_lock, &attr))){
        S3FS_PRN_CRIT("failed to init token_lock: %d", result);
        abort();
    }
    is_lock_init = true;
}

S3fsCred::~S3fsCred()
{
    UnloadExtCredLib();
    if(is_lock_init){
        int result;
        if(0 != (result = pthread_mutex_destroy(&token_lock))){
            S3FS_PRN_CRIT("failed to destroy token_lock: %d", result);
            abort();
        }
        is_lock_init = false;
    }
}

//-------------------------------------------------------------------
// Methods : Access member variables
//-------------------------------------------------------------------
bool S3fsCred::SetPasswdFile(const char* file)
{
    if(!file || strlen(file) == 0){
        return false;
    }
    passwd_file = file;

    return true;
}

bool S3fsCred::IsSetPasswdFile() const
{
    return !passwd_file.empty();
}

bool S3fsCred::SetRamRoleMetadataType(bool flag)
{
    bool old = load_ramrole;
    load_ramrole = flag;
    return old;
}

bool S3fsCred::SetAccessKey(const char* AccessKeyId_, const char* SecretAccessKey_, AutoLock::Type type)
{
    AutoLock auto_lock(&token_lock, type);

    if((!AccessKeyId_ || '\0' == AccessKeyId_[0]) || !SecretAccessKey_ || '\0' == SecretAccessKey_[0]){
        return false;
    }
    AccessKeyId     = AccessKeyId_;
    AccessKeySecret = SecretAccessKey_;

    return true;
}

bool S3fsCred::SetAccessKeyWithSessionToken(const char* AccessKeyId_, const char* SecretAccessKey_, const char * SessionToken_, AutoLock::Type type)
{
    AutoLock auto_lock(&token_lock, type);

    bool access_key_is_empty        = !AccessKeyId_     || '\0' == AccessKeyId_[0];
    bool secret_access_key_is_empty = !SecretAccessKey_ || '\0' == SecretAccessKey_[0];
    bool session_token_is_empty     = !SessionToken_    || '\0' == SessionToken_[0];

    if(access_key_is_empty || secret_access_key_is_empty || session_token_is_empty){
        return false;
    }
    AccessKeyId      = AccessKeyId_;
    AccessKeySecret  = SecretAccessKey_;
    SecurityToken      = SessionToken_;
    is_use_session_token= true;

    return true;
}

bool S3fsCred::IsSetAccessKeys(AutoLock::Type type)
{
    AutoLock auto_lock(&token_lock, type);

    return IsSetRAMRole(AutoLock::ALREADY_LOCKED) || ((!AccessKeyId.empty() || false) && !AccessKeySecret.empty());
}

bool S3fsCred::SetIsUseSessionToken(bool flag)
{
    bool old = is_use_session_token;
    is_use_session_token = flag;
    return old;
}

bool S3fsCred::SetRAMRole(const char* role, AutoLock::Type type)
{
    AutoLock auto_lock(&token_lock, type);

    RAM_role = role ? role : "";
    return true;
}

std::string S3fsCred::GetRAMRole(AutoLock::Type type)
{
    AutoLock auto_lock(&token_lock, type);

    return RAM_role;
}

bool S3fsCred::IsSetRAMRole(AutoLock::Type type)
{
    AutoLock auto_lock(&token_lock, type);

    return !RAM_role.empty();
}

size_t S3fsCred::SetRAMFieldCount(size_t field_count)
{
    size_t old = RAM_field_count;
    RAM_field_count = field_count;
    return old;
}

std::string S3fsCred::SetRAMCredentialsURL(const char* url)
{
    std::string old = RAM_cred_url;
    RAM_cred_url = url ? url : "";
    return old;
}

std::string S3fsCred::SetIAMTokenField(const char* token_field)
{
    std::string old = RAM_token_field;
    RAM_token_field = token_field ? token_field : "";
    return old;
}

std::string S3fsCred::SetIAMExpiryField(const char* expiry_field)
{
    std::string old = RAM_expiry_field;
    RAM_expiry_field = expiry_field ? expiry_field : "";
    return old;
}

bool S3fsCred::GetRAMCredentialsURL(std::string& url, bool check_ram_role, AutoLock::Type type)
{
    // check
    if(check_ram_role ){
        if(!IsSetRAMRole(type)) {
            S3FS_PRN_ERR("RAM role name is empty.");
            return false;
        }
        S3FS_PRN_INFO3("[RAM role=%s]", GetRAMRole(type).c_str());
    }

    {
        // [NOTE]
        // To avoid deadlocking, do not manipulate the S3fsCred object
        // in the S3fsCurl::GetIAMv2ApiToken method (when retrying).
        //
        AutoLock auto_lock(&token_lock, type);      // Lock for 

        if(check_ram_role){
            url = RAM_cred_url + GetRAMRole(AutoLock::ALREADY_LOCKED);
        }else{
            url = RAM_cred_url;
        }
    }
    return true;
}


// [NOTE]
// Currently, token_lock is always locked before calling this method,
// and this method calls the S3fsCurl::GetRAMCredentials method.
// Currently, when the request fails and retries in the process of
// S3fsCurl::GetRAMCredentials, does not use the S3fsCred object in
// retry logic.
// Be careful not to deadlock whenever you change this logic.
//
bool S3fsCred::LoadRAMCredentials(AutoLock::Type type)
{
    // url(check ram role)
    std::string url;

    AutoLock auto_lock(&token_lock, type);

    if(!GetRAMCredentialsURL(url, true, AutoLock::ALREADY_LOCKED)){
        return false;
    }

    S3fsCurl    s3fscurl;
    std::string response;
    if(!s3fscurl.GetRAMCredentials(url.c_str(), NULL, NULL, response)){
        return false;
    }

    if(!SetRAMCredentials(response.c_str(), AutoLock::ALREADY_LOCKED)){
        S3FS_PRN_ERR("Something error occurred, could not set RAM role name.");
        return false;
    }
	return true;
}

//
// load ecs ram role name from http://100.100.100.200/latest/meta-data/ram/security-credentials/
//
bool S3fsCred::LoadIAMRoleFromMetaData()
{
    AutoLock auto_lock(&token_lock);

    if(load_ramrole){
        // url(not check ram role)
        std::string url;

        if(!GetRAMCredentialsURL(url, false, AutoLock::ALREADY_LOCKED)){
            return false;
        }

        S3fsCurl    s3fscurl;
        std::string token;
        if(!s3fscurl.GetRAMRoleFromMetaData(url.c_str(), NULL, token)){
            return false;
        }

        if(!SetRAMRoleFromMetaData(token.c_str(), AutoLock::ALREADY_LOCKED)){
            S3FS_PRN_ERR("Something error occurred, could not set RAM role name.");
            return false;
        }
        S3FS_PRN_INFO("loaded RAM role name = %s", GetRAMRole(AutoLock::ALREADY_LOCKED).c_str());
    }
    return true;
}

bool S3fsCred::SetRAMCredentials(const char* response, AutoLock::Type type)
{
    S3FS_PRN_INFO3("RAM credential response = \"%s\"", response);

    ramcredmap_t keyval;

    if(!ParseRAMCredentialResponse(response, keyval)){
        return false;
    }

    if(RAM_field_count != keyval.size()){
        return false;
    }

    AutoLock auto_lock(&token_lock, type);

    SecurityToken = keyval[RAM_token_field];

    AccessKeyId       = keyval[std::string(S3fsCred::RAMCRED_ACCESSKEYID)];
    AccessKeySecret   = keyval[std::string(S3fsCred::RAMCRED_SECRETACCESSKEY)];
    SecurityTokenExpire = cvtIAMExpireStringToTime(keyval[RAM_expiry_field].c_str());

    return true;
}

bool S3fsCred::SetRAMRoleFromMetaData(const char* response, AutoLock::Type type)
{
    S3FS_PRN_INFO3("RAM role name response = \"%s\"", response ? response : "(null)");

    std::string rolename;
    if(!S3fsCred::ParseRAMRoleFromMetaDataResponse(response, rolename)){
        return false;
    }

    SetRAMRole(rolename.c_str(), type);
    return true;
}

//-------------------------------------------------------------------
// Methods : for Credentials
//-------------------------------------------------------------------
//
// Check passwd file readable
//
bool S3fsCred::IsReadablePasswdFile()
{
    if(passwd_file.empty()){
        return false;
    }

    std::ifstream PF(passwd_file.c_str());
    if(!PF.good()){
        return false;
    }
    PF.close();

    return true;
}

//
// S3fsCred::CheckPasswdFilePerms
//
// expect that global passwd_file variable contains
// a non-empty value and is readable by the current user
//
// Check for too permissive access to the file
// help save users from themselves via a security hole
//
// only two options: return or error out
//
bool S3fsCred::CheckPasswdFilePerms()
{
    struct stat info;

    // let's get the file info
    if(stat(passwd_file.c_str(), &info) != 0){
        S3FS_PRN_EXIT("unexpected error from stat(%s): %s", passwd_file.c_str(), strerror(errno));
        return false;
    }

	// Check readable
    if(!IsReadablePasswdFile()){
        S3FS_PRN_EXIT("S3fs passwd file \"%s\" is not readable.", passwd_file.c_str());
        return false;
    }

    // return error if any file has others permissions
    if( (info.st_mode & S_IROTH) ||
        (info.st_mode & S_IWOTH) ||
        (info.st_mode & S_IXOTH)) {
        S3FS_PRN_EXIT("credentials file %s should not have others permissions.", passwd_file.c_str());
        return false;
    }

    // Any local file should not have any group permissions
    // default passwd file can have group permissions
    if(passwd_file != DEFAULT_PASSWD_FILE){
        if( (info.st_mode & S_IRGRP) ||
            (info.st_mode & S_IWGRP) ||
            (info.st_mode & S_IXGRP)) {
            S3FS_PRN_EXIT("credentials file %s should not have group permissions.", passwd_file.c_str());
            return false;
        }
    }else{
        // default passwd file does not allow group write.
        if((info.st_mode & S_IWGRP)){
            S3FS_PRN_EXIT("credentials file %s should not have group writable permissions.", passwd_file.c_str());
            return false;
        }
    }
    if((info.st_mode & S_IXUSR) || (info.st_mode & S_IXGRP)){
        S3FS_PRN_EXIT("credentials file %s should not have executable permissions.", passwd_file.c_str());
        return false;
    }
    return true;
}

//
// Read and Parse passwd file
//
// The line of the password file is one of the following formats:
//   (1) "accesskey:secretkey"         : format for default(all) access key/secret key
//   (2) "bucket:accesskey:secretkey"  : format for bucket's access key/secret key
//   (3) "key=value"                   : Content-dependent KeyValue contents
//
// This function sets result into bucketkvmap_t, it bucket name and key&value mapping.
// If bucket name is empty(1 or 3 format), bucket name for mapping is set "\t" or "".
//
// Return: true  - Succeed parsing
//         false - Should shutdown immediately
//
bool S3fsCred::ParsePasswdFile(bucketkvmap_t& resmap)
{
    std::string          line;
    size_t               first_pos;
    readline_t           linelist;
    readline_t::iterator iter;

    // open passwd file
    std::ifstream PF(passwd_file.c_str());
    if(!PF.good()){
        S3FS_PRN_EXIT("could not open passwd file : %s", passwd_file.c_str());
        return false;
    }

    // read each line
    while(getline(PF, line)){
        line = trim(line);
        if(line.empty()){
            continue;
        }
        if('#' == line[0]){
            continue;
        }
        if(std::string::npos != line.find_first_of(" \t")){
            S3FS_PRN_EXIT("invalid line in passwd file, found whitespace character.");
            return false;
        }
        if('[' == line[0]){
            S3FS_PRN_EXIT("invalid line in passwd file, found a bracket \"[\" character.");
            return false;
        }
        linelist.push_back(line);
    }

    // read '=' type
    kvmap_t kv;
    for(iter = linelist.begin(); iter != linelist.end(); ++iter){
        first_pos = iter->find_first_of('=');
        if(first_pos == std::string::npos){
            continue;
        }
        // formatted by "key=val"
        std::string key = trim(iter->substr(0, first_pos));
        std::string val = trim(iter->substr(first_pos + 1, std::string::npos));
        if(key.empty()){
            continue;
        }
        if(kv.end() != kv.find(key)){
            S3FS_PRN_WARN("same key name(%s) found in passwd file, skip this.", key.c_str());
            continue;
        }
        kv[key] = val;
    }
    // set special key name
    resmap[S3fsCred::KEYVAL_FIELDS_TYPE] = kv;

    // read ':' type
    for(iter = linelist.begin(); iter != linelist.end(); ++iter){
        first_pos       = iter->find_first_of(':');
        size_t last_pos = iter->find_last_of(':');
        if(first_pos == std::string::npos){
            continue;
        }
        std::string bucketname;
        std::string accesskey;
        std::string secret;
        if(first_pos != last_pos){
            // formatted by "bucket:accesskey:secretkey"
            bucketname= trim(iter->substr(0, first_pos));
            accesskey = trim(iter->substr(first_pos + 1, last_pos - first_pos - 1));
            secret    = trim(iter->substr(last_pos + 1, std::string::npos));
        }else{
            // formatted by "accesskey:secretkey"
            bucketname= S3fsCred::ALLBUCKET_FIELDS_TYPE;
            accesskey = trim(iter->substr(0, first_pos));
            secret    = trim(iter->substr(first_pos + 1, std::string::npos));
        }
        if(resmap.end() != resmap.find(bucketname)){
            S3FS_PRN_EXIT("there are multiple entries for the same bucket(%s) in the passwd file.", (bucketname.empty() ? "default" : bucketname.c_str()));
            return false;
        }
        kv.clear();
        kv[S3fsCred::KEY_ACCESSKEYID] = accesskey;
        kv[S3fsCred::KEY_SECRETKEY]   = secret;
        resmap[bucketname] = kv;
    }
    return true;
}

//
// ReadPasswdFile
//
// Support for per bucket credentials
//
// Format for the credentials file:
// [bucket:]AccessKeyId:AccessKeySecret
//
// Lines beginning with # are considered comments
// and ignored, as are empty lines
//
// Uncommented lines without the ":" character are flagged as
// an error, so are lines with spaces or tabs
//
// only one default key pair is allowed, but not required
//
bool S3fsCred::ReadPasswdFile(AutoLock::Type type)
{
    bucketkvmap_t bucketmap;
    kvmap_t       keyval;

    // if you got here, the password file
    // exists and is readable by the
    // current user, check for permissions
    if(!CheckPasswdFilePerms()){
        return false;
    }

    //
    // parse passwd file
    //
    if(!ParsePasswdFile(bucketmap)){
        return false;
    }

    //
    // check key=value type format.
    //
    bucketkvmap_t::iterator it = bucketmap.find(S3fsCred::KEYVAL_FIELDS_TYPE);
    if(bucketmap.end() != it){
        // aws format
        std::string access_key_id;
        std::string secret_access_key;
        int result = CheckCredentialOssFormat(it->second, access_key_id, secret_access_key);
        if(-1 == result){
            return false;
        }else if(1 == result){
            // found ascess(secret) keys
            if(!SetAccessKey(access_key_id.c_str(), secret_access_key.c_str(), type)){
                S3FS_PRN_EXIT("failed to set access key/secret key.");
                return false;
            }
            return true;
        }
    }

    std::string bucket_key = S3fsCred::ALLBUCKET_FIELDS_TYPE;
    if(!S3fsCred::bucket_name.empty() && bucketmap.end() != bucketmap.find(S3fsCred::bucket_name)){
        bucket_key = S3fsCred::bucket_name;
    }

    it = bucketmap.find(bucket_key);
    if(bucketmap.end() == it){
        S3FS_PRN_EXIT("Not found access key/secret key in passwd file.");
        return false;
    }
    keyval = it->second;
    kvmap_t::iterator accesskeyid_it = keyval.find(S3fsCred::KEY_ACCESSKEYID);
    kvmap_t::iterator secretkey_it   = keyval.find(S3fsCred::KEY_SECRETKEY);
    if(keyval.end() == accesskeyid_it || keyval.end() == secretkey_it){
        S3FS_PRN_EXIT("Not found access key/secret key in passwd file.");
        return false;
    }

    if(!SetAccessKey(accesskeyid_it->second.c_str(), secretkey_it->second.c_str(), type)){
        S3FS_PRN_EXIT("failed to set internal data for access key/secret key from passwd file.");
        return false;
    }
    return true;
}

//
// Return:  1 - OK(could read and set accesskey etc.)
//          0 - NG(could not read)
//         -1 - Should shutdown immediately
//
int S3fsCred::CheckCredentialOssFormat(const kvmap_t& kvmap, std::string& access_key_id, std::string& secret_access_key)
{
    std::string str1(S3fsCred::KEY_ACCESSKEYID);
    std::string str2(S3fsCred::KEY_SECRETKEY);

    if(kvmap.empty()){
        return 0;
    }
    kvmap_t::const_iterator str1_it = kvmap.find(str1);
    kvmap_t::const_iterator str2_it = kvmap.find(str2);
    if(kvmap.end() == str1_it && kvmap.end() == str2_it){
        return 0;
    }
    if(kvmap.end() == str1_it || kvmap.end() == str2_it){
        S3FS_PRN_EXIT("Accesskey or Secretkey is not specified.");
        return -1;
    }
    access_key_id     = str1_it->second;
    secret_access_key = str2_it->second;

    return 1;
}


//
// InitialCredentials
//
// called only when were are not mounting a
// public bucket
//
// Here is the order precedence for getting the
// keys:
//
// 1 - from the command line  (security risk)
// 2 - from a password file specified on the command line
// 3 - from environment variables
// 3a -from the OSS_CREDENTIAL_FILE environment variable
// 4 - from the users ~/.passwd-ossfs
// 5 - from /etc/passwd-ossfs
//
bool S3fsCred::InitialCredentials()
{
    // should be redundant
    if(S3fsCurl::IsPublicBucket()){
        return true;
    }

    // access key loading is deferred
    if(load_ramrole || IsSetExtCredLib()){
        return true;
    }

    // 1 - keys specified on the command line
    if(IsSetAccessKeys(AutoLock::NONE)){
        return true;
    }

    // 2 - was specified on the command line
    if(IsSetPasswdFile()){
        if(!ReadPasswdFile(AutoLock::NONE)){
            return false;
        }
        return true;
    }

    // 3  - environment variables
    char* OSSACCESSKEYID     = getenv("OSSACCESSKEYID") ?     getenv("OSSACCESSKEYID") :     getenv("OSS_ACCESS_KEY_ID");
    char* OSSSECRETACCESSKEY = getenv("OSSSECRETACCESSKEY") ? getenv("OSSSECRETACCESSKEY") : getenv("OSS_ACCESS_KEY_SECRET");
    char* OSSSESSIONTOKEN    = getenv("OSSSESSIONTOKEN") ?   getenv("OSSSESSIONTOKEN") :   getenv("OSS_SESSION_TOKEN");

    if(OSSACCESSKEYID != NULL || OSSSECRETACCESSKEY != NULL){
        if( (OSSACCESSKEYID == NULL && OSSSECRETACCESSKEY != NULL) ||
            (OSSACCESSKEYID != NULL && OSSSECRETACCESSKEY == NULL) ){
            S3FS_PRN_EXIT("both environment variables OSSACCESSKEYID and OSSSECRETACCESSKEY must be set together.");
            return false;
        }
        S3FS_PRN_INFO2("access key from env variables");
        if(OSSSESSIONTOKEN != NULL){
            S3FS_PRN_INFO2("session token is available");
            if(!SetAccessKeyWithSessionToken(OSSACCESSKEYID, OSSSECRETACCESSKEY, OSSSESSIONTOKEN, AutoLock::NONE)){
                 S3FS_PRN_EXIT("session token is invalid.");
                 return false;
            }
        }else{
            S3FS_PRN_INFO2("session token is not available");
            if(is_use_session_token){
                S3FS_PRN_EXIT("environment variable OSSSESSIONTOKEN is expected to be set.");
                return false;
            }
        }
        if(!SetAccessKey(OSSACCESSKEYID, OSSSECRETACCESSKEY, AutoLock::NONE)){
            S3FS_PRN_EXIT("if one access key is specified, both keys need to be specified.");
            return false;
        }
        return true;
    }

    // 3a - from the OSS_CREDENTIAL_FILE environment variable
    char* OSS_CREDENTIAL_FILE = getenv("OSS_CREDENTIAL_FILE");
    if(OSS_CREDENTIAL_FILE != NULL){
        passwd_file = OSS_CREDENTIAL_FILE;
        if(IsSetPasswdFile()){
            if(!IsReadablePasswdFile()){
                S3FS_PRN_EXIT("OSS_CREDENTIAL_FILE: \"%s\" is not readable.", passwd_file.c_str());
                return false;
            }
            if(!ReadPasswdFile(AutoLock::NONE)){
                return false;
            }
            return true;
        }
    }

    // 4 - from the default location in the users home directory
    char* HOME = getenv("HOME");
    if(HOME != NULL){
        passwd_file = HOME;
        passwd_file += "/.passwd-ossfs";
        if(IsReadablePasswdFile()){
            if(!ReadPasswdFile(AutoLock::NONE)){
                return false;
            }

            // It is possible that the user's file was there but
            // contained no key pairs i.e. commented out
            // in that case, go look in the final location
            if(IsSetAccessKeys(AutoLock::NONE)){
                return true;
            }
        }
    }

    // 5 - from the system default location
    passwd_file = DEFAULT_PASSWD_FILE;
    if(IsReadablePasswdFile()){
        if(!ReadPasswdFile(AutoLock::NONE)){
            return false;
        }
        return true;
    }

    S3FS_PRN_EXIT("could not determine how to establish security credentials.");
    return false;
}

//-------------------------------------------------------------------
// Methods : for IAM
//-------------------------------------------------------------------
bool S3fsCred::ParseRAMCredentialResponse(const char* response, ramcredmap_t& keyval)
{
    if(!response){
      return false;
    }
    std::istringstream sscred(response);
    std::string        oneline;
    keyval.clear();
    while(getline(sscred, oneline, ',')){
        std::string::size_type pos;
        std::string            key;
        std::string            val;
        if(std::string::npos != (pos = oneline.find(S3fsCred::RAMCRED_ACCESSKEYID))){
            key = S3fsCred::RAMCRED_ACCESSKEYID;
        }else if(std::string::npos != (pos = oneline.find(S3fsCred::RAMCRED_SECRETACCESSKEY))){
            key = S3fsCred::RAMCRED_SECRETACCESSKEY;
        }else if(std::string::npos != (pos = oneline.find(S3fsCred::RAMCRED_ACCESSKEYSECRET))){
            key = S3fsCred::RAMCRED_SECRETACCESSKEY;
        }
        else if(std::string::npos != (pos = oneline.find(RAM_token_field))){
            key = RAM_token_field;
        }else if(std::string::npos != (pos = oneline.find(RAM_expiry_field))){
            key = RAM_expiry_field;
        }else if(std::string::npos != (pos = oneline.find(S3fsCred::RAMCRED_ROLEARN))){
            key = S3fsCred::RAMCRED_ROLEARN;
        }else{
            continue;
        }
        if(std::string::npos == (pos = oneline.find(':', pos + key.length()))){
            continue;
        }

        // parse std::string value (starts and ends with quotes)
        if(std::string::npos == (pos = oneline.find('\"', pos))){
            continue;
        }
        oneline.erase(0, pos+1);
        if(std::string::npos == (pos = oneline.find('\"'))){
            continue;
        }
        val = oneline.substr(0, pos);

        keyval[key] = val;
    }
    return true;
}

bool S3fsCred::CheckIAMCredentialUpdate(std::string* access_key_id, std::string* secret_access_key, std::string* access_token)
{
    AutoLock auto_lock(&token_lock);

    if(IsSetExtCredLib() || IsSetRAMRole(AutoLock::ALREADY_LOCKED)){
        if(SecurityTokenExpire < (time(NULL) + S3fsCred::RAM_EXPIRE_MERGIN)){
            S3FS_PRN_INFO("Security Token refreshing...");

            // update
            if(!IsSetExtCredLib()){
                if(!LoadRAMCredentials(AutoLock::ALREADY_LOCKED)){
                    S3FS_PRN_ERR("Security Token refresh failed");
                    return false;
                }
            }else{
                if(!UpdateExtCredentials(AutoLock::ALREADY_LOCKED)){
                    S3FS_PRN_ERR("Access Token refresh by %s(external credential library) failed", credlib.c_str());
                    return false;
                }
            }
            S3FS_PRN_INFO("Security Token refreshed");
        }
    }

    // set
    if(access_key_id){
        *access_key_id = AccessKeyId;
    }
    if(secret_access_key){
        *secret_access_key = AccessKeySecret;
    }
    if(access_token){
        if(IsSetRAMRole(AutoLock::ALREADY_LOCKED) || IsSetExtCredLib() || is_use_session_token){
            *access_token = SecurityToken;
        }else{
            access_token->erase();
        }
    }

    return true;
}

const char* S3fsCred::GetCredFuncVersion(bool detail) const
{
    static const char errVersion[] = "unknown";

    if(!pFuncCredVersion){
        return errVersion;
    }
    return (*pFuncCredVersion)(detail);
}

//-------------------------------------------------------------------
// Methods : External Credential Library
//-------------------------------------------------------------------
bool S3fsCred::SetExtCredLib(const char* arg)
{
    if(!arg || strlen(arg) == 0){
        return false;
    }
    credlib = arg;

    return true;
}

bool S3fsCred::IsSetExtCredLib() const
{
    return !credlib.empty();
}

bool S3fsCred::SetExtCredLibOpts(const char* args)
{
    if(!args || strlen(args) == 0){
        return false;
    }
    credlib_opts = args;

    return true;
}

bool S3fsCred::IsSetExtCredLibOpts() const
{
    return !credlib_opts.empty();
}

bool S3fsCred::InitExtCredLib()
{
    if(!LoadExtCredLib()){
        return false;
    }
    // Initialize library
    if(!pFuncCredInit){
        S3FS_PRN_CRIT("\"InitS3fsCredential\" function pointer is NULL, why?");
        UnloadExtCredLib();
        return false;
    }

    const char* popts   = credlib_opts.empty() ? NULL : credlib_opts.c_str();
    char*       perrstr = NULL;
    if(!(*pFuncCredInit)(popts, &perrstr)){
        S3FS_PRN_ERR("Could not initialize %s(external credential library) by \"InitS3fsCredential\" function : %s", credlib.c_str(), perrstr ? perrstr : "unknown");
        // cppcheck-suppress unmatchedSuppression
        // cppcheck-suppress knownConditionTrueFalse
        if(perrstr){
            free(perrstr);
        }
        UnloadExtCredLib();
        return false;
    }
    // cppcheck-suppress unmatchedSuppression
    // cppcheck-suppress knownConditionTrueFalse
    if(perrstr){
        free(perrstr);
    }

    return true;
}

bool S3fsCred::LoadExtCredLib()
{
    if(credlib.empty()){
        return false;
    }
    UnloadExtCredLib();

    S3FS_PRN_INFO("Load External Credential Library : %s", credlib.c_str());

    // Open Library
    //
    // Search Library: (RPATH ->) LD_LIBRARY_PATH -> (RUNPATH ->) /etc/ld.so.cache -> /lib -> /usr/lib
    //
    if(NULL == (hExtCredLib = dlopen(credlib.c_str(), RTLD_LAZY))){
        const char* preason = dlerror();
        S3FS_PRN_ERR("Could not load %s(external credential library) by error : %s", credlib.c_str(), preason ? preason : "unknown");
        return false;
    }

    // Set function pointers
    if(NULL == (pFuncCredVersion = reinterpret_cast<fp_VersionS3fsCredential>(dlsym(hExtCredLib, "VersionS3fsCredential")))){
        S3FS_PRN_ERR("%s(external credential library) does not have \"VersionS3fsCredential\" function which is required.", credlib.c_str());
        UnloadExtCredLib();
        return false;
    }

    if(NULL == (pFuncCredUpdate = reinterpret_cast<fp_UpdateS3fsCredential>(dlsym(hExtCredLib, "UpdateS3fsCredential")))){
        S3FS_PRN_ERR("%s(external credential library) does not have \"UpdateS3fsCredential\" function which is required.", credlib.c_str());
        UnloadExtCredLib();
        return false;
    }

    if(NULL == (pFuncCredInit = reinterpret_cast<fp_InitS3fsCredential>(dlsym(hExtCredLib, "InitS3fsCredential")))){
        S3FS_PRN_INFO("%s(external credential library) does not have \"InitS3fsCredential\" function which is optional.", credlib.c_str());
        pFuncCredInit = InitS3fsCredential;     // set built-in function
    }

    if(NULL == (pFuncCredFree = reinterpret_cast<fp_FreeS3fsCredential>(dlsym(hExtCredLib, "FreeS3fsCredential")))){
        S3FS_PRN_INFO("%s(external credential library) does not have \"FreeS3fsCredential\" function which is optional.", credlib.c_str());
        pFuncCredFree = FreeS3fsCredential;     // set built-in function
    }
    S3FS_PRN_INFO("Succeed loading External Credential Library : %s", credlib.c_str());

    return true;
}

bool S3fsCred::UnloadExtCredLib()
{
    if(hExtCredLib){
        S3FS_PRN_INFO("Unload External Credential Library : %s", credlib.c_str());

        // Uninitialize library
        if(!pFuncCredFree){
            S3FS_PRN_CRIT("\"FreeS3fsCredential\" function pointer is NULL, why?");
        }else{
            char* perrstr = NULL;
            if(!(*pFuncCredFree)(&perrstr)){
                S3FS_PRN_ERR("Could not uninitialize by \"FreeS3fsCredential\" function : %s", perrstr ? perrstr : "unknown");
            }
            // cppcheck-suppress unmatchedSuppression
            // cppcheck-suppress knownConditionTrueFalse
            if(perrstr){
                free(perrstr);
            }
        }

        // reset
        pFuncCredVersion = VersionS3fsCredential;
        pFuncCredInit    = InitS3fsCredential;
        pFuncCredFree    = FreeS3fsCredential;
        pFuncCredUpdate  = UpdateS3fsCredential;

        // close
        dlclose(hExtCredLib);
        hExtCredLib = NULL;
    }
    return true;
}

bool S3fsCred::UpdateExtCredentials(AutoLock::Type type)
{
    if(!hExtCredLib){
        S3FS_PRN_CRIT("External Credential Library is not loaded, why?");
        return false;
    }

    AutoLock auto_lock(&token_lock, type);

    char* paccess_key_id     = nullptr;
    char* pserect_access_key = nullptr;
    char* paccess_token      = nullptr;
    char* perrstr            = nullptr;
    long long token_expire   = 0;
    bool result = (*pFuncCredUpdate)(&paccess_key_id, &pserect_access_key, &paccess_token, &token_expire, &perrstr);
    if(!result){
        // error occurred
        S3FS_PRN_ERR("Could not update credential by \"UpdateS3fsCredential\" function : %s", perrstr ? perrstr : "unknown");

    // cppcheck-suppress unmatchedSuppression
    // cppcheck-suppress knownConditionTrueFalse
    }else if(!paccess_key_id || !pserect_access_key || !paccess_token || token_expire <= 0){
        // some variables are wrong
        S3FS_PRN_ERR("After updating credential by \"UpdateS3fsCredential\" function, but some variables are wrong : paccess_key_id=%p, pserect_access_key=%p, paccess_token=%p, token_expire=%lld", paccess_key_id, pserect_access_key, paccess_token, token_expire);
        result = false;
    }else{
        // succeed updating
        AccessKeyId       = paccess_key_id;
        AccessKeySecret   = pserect_access_key;
        SecurityToken       = paccess_token;
        SecurityTokenExpire = token_expire;
    }

    // clean
    // cppcheck-suppress unmatchedSuppression
    // cppcheck-suppress knownConditionTrueFalse
    if(paccess_key_id){
        free(paccess_key_id);
    }
    // cppcheck-suppress unmatchedSuppression
    // cppcheck-suppress knownConditionTrueFalse
    if(pserect_access_key){
        free(pserect_access_key);
    }
    // cppcheck-suppress unmatchedSuppression
    // cppcheck-suppress knownConditionTrueFalse
    if(paccess_token){
        free(paccess_token);
    }
    // cppcheck-suppress unmatchedSuppression
    // cppcheck-suppress knownConditionTrueFalse
    if(perrstr){
        free(perrstr);
    }

    return result;
}

//-------------------------------------------------------------------
// Methods: Option detection
//-------------------------------------------------------------------
// return value:  1 = Not processed as it is not a option for this class
//                0 = The option was detected and processed appropriately
//               -1 = Processing cannot be continued because a fatal error was detected
//
int S3fsCred::DetectParam(const char* arg)
{
    if(!arg){
        S3FS_PRN_EXIT("parameter arg is empty(null)");
        return -1;
    }

    if(is_prefix(arg, "passwd_file=")){
        SetPasswdFile(strchr(arg, '=') + sizeof(char));
        set_builtin_cred_opts = true;
        return 0;
    }

    if(0 == strcmp(arg, "use_session_token")){
        SetIsUseSessionToken(true);
        set_builtin_cred_opts = true;
        return 0;
    }

    if(is_prefix(arg, "ram_role")){
        if(0 == strcmp(arg, "ram_role") || 0 == strcmp(arg, "ram_role=auto")){
            // loading RAM role name in s3fs_init(), because we need to wait initializing curl.
            SetRamRoleMetadataType(true);
            set_builtin_cred_opts = true;
            return 0;

        }else if(is_prefix(arg, "ram_role=")){
            const char* role = strchr(arg, '=') + sizeof(char);
            SetRAMRole(role, AutoLock::NONE);
            SetRamRoleMetadataType(false);
            // compatible with old mode
            set_builtin_cred_opts = true;
            if(0 == strncmp(role, "http", 4)){
                SetRAMCredentialsURL("");
            }
            return 0;
        }
    }

    if(is_prefix(arg, "credlib=")){
        if(!SetExtCredLib(strchr(arg, '=') + sizeof(char))){
             S3FS_PRN_EXIT("failed to set credlib option : %s", (strchr(arg, '=') + sizeof(char)));
             return -1;
        }
        return 0;
    }

    if(is_prefix(arg, "credlib_opts=")){
        if(!SetExtCredLibOpts(strchr(arg, '=') + sizeof(char))){
             S3FS_PRN_EXIT("failed to set credlib_opts option : %s", (strchr(arg, '=') + sizeof(char)));
             return -1;
        }
        return 0;
    }

    return 1;
}

//-------------------------------------------------------------------
// Methods : check parameters
//-------------------------------------------------------------------
//
// Checking forbidden parameters for bucket
//
bool S3fsCred::CheckForbiddenBucketParams()
{
    // The first plain argument is the bucket
    if(bucket_name.empty()){
        S3FS_PRN_EXIT("missing BUCKET argument.");
        return false;
    }

    // bucket names cannot contain upper case characters in virtual-hosted style
    if(!pathrequeststyle && (lower(bucket_name) != bucket_name)){
        S3FS_PRN_EXIT("BUCKET %s, name not compatible with virtual-hosted style.", bucket_name.c_str());
        return false;
    }

    // check bucket name for illegal characters
    size_t found = bucket_name.find_first_of("/:\\;!@#$%^&*?|+=");
    if(found != std::string::npos){
        S3FS_PRN_EXIT("BUCKET %s -- bucket name contains an illegal character.", bucket_name.c_str());
        return false;
    }

    return true;
}

//
// Check the combination of parameters
//
bool S3fsCred::CheckAllParams()
{
    //
    // Checking forbidden parameters for bucket
    //
    if(!CheckForbiddenBucketParams()){
        return false;
    }

    // error checking of command line arguments for compatibility
    if(S3fsCurl::IsPublicBucket() && IsSetAccessKeys(AutoLock::NONE)){
        S3FS_PRN_EXIT("specifying both public_bucket and the access keys options is invalid.");
        return false;
    }

    if(IsSetPasswdFile() && IsSetAccessKeys(AutoLock::NONE)){
        S3FS_PRN_EXIT("specifying both passwd_file and the access keys options is invalid.");
        return false;
    }

    if(!S3fsCurl::IsPublicBucket() && !load_ramrole && !IsSetExtCredLib()){
        if(!InitialCredentials()){
            return false;
        }
        if(!IsSetAccessKeys(AutoLock::NONE)){
            S3FS_PRN_EXIT("could not establish security credentials, check documentation.");
            return false;
        }
        // More error checking on the access key pair can be done
        // like checking for appropriate lengths and characters  
    }

    // check External Credential Library
    //
    // [NOTE]
    // If credlib(_opts) option (for External Credential Library) is specified,
    // no other Credential related options can be specified. It is exclusive.
    //
    if(set_builtin_cred_opts && (IsSetExtCredLib() || IsSetExtCredLibOpts())){
        S3FS_PRN_EXIT("The \"credlib\" or \"credlib_opts\" option and other credential-related options(passwd_file, iam_role, profile, use_session_token, ecs, imdsv1only, ibm_iam_auth, ibm_iam_endpoint, etc) cannot be specified together.");
        return false;
    }

    // Load and Initialize external credential library
    if(IsSetExtCredLib() || IsSetExtCredLibOpts()){
        if(!IsSetExtCredLib()){
            S3FS_PRN_EXIT("The \"credlib_opts\"(%s) is specifyed but \"credlib\" option is not specified.", credlib_opts.c_str());
            return false;
        }

        if(!InitExtCredLib()){
             S3FS_PRN_EXIT("failed to load the library specified by the option credlib(%s, %s).", credlib.c_str(), credlib_opts.c_str());
             return false;
        }
        S3FS_PRN_INFO("Loaded External Credential Library:\n%s", GetCredFuncVersion(true));
    }

    return true;
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
