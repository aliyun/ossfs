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
    RAM_role("")
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

bool S3fsCred::IsSetPasswdFile()
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
        S3FS_PRN_EXIT("unexpected error from stat(%s).", passwd_file.c_str());
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
    if(load_ramrole){
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
    char* OSSSESSIONTOKEN    = getenv("OSSSSESSIONTOKEN") ?   getenv("OSSSSESSIONTOKEN") :   getenv("OSS_SESSION_TOKEN");

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

    if(IsSetRAMRole(AutoLock::ALREADY_LOCKED)){
        if(SecurityTokenExpire < (time(NULL) + S3fsCred::RAM_EXPIRE_MERGIN)){
            S3FS_PRN_INFO("Security Token refreshing...");

            // update
            if(!LoadRAMCredentials(AutoLock::ALREADY_LOCKED)){
                S3FS_PRN_ERR("Security Token refresh failed");
                return false;
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
        if(IsSetRAMRole(AutoLock::ALREADY_LOCKED) || is_use_session_token){
            *access_token = SecurityToken;
        }else{
            access_token->erase();
        }
    }

    return true;
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
        return 0;
    }

    if(0 == strcmp(arg, "use_session_token")){
        SetIsUseSessionToken(true);
        return 0;
    }

    if(is_prefix(arg, "ram_role")){
        if(0 == strcmp(arg, "ram_role") || 0 == strcmp(arg, "ram_role=auto")){
            // loading RAM role name in s3fs_init(), because we need to wait initializing curl.
            SetRamRoleMetadataType(true);
            return 0;

        }else if(is_prefix(arg, "ram_role=")){
            const char* role = strchr(arg, '=') + sizeof(char);
            SetRAMRole(role, AutoLock::NONE);
            SetRamRoleMetadataType(false);
            // compatible with old mode
            if(0 == strncmp(role, "http", 4)){
                SetRAMCredentialsURL("");
            }
            return 0;
        }
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

    if(!S3fsCurl::IsPublicBucket() && !load_ramrole){
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
