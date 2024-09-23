// This is just an example of testing option credlib and credlib_opts.
// compile: g++ testlib.cpp -l curl -fPIC -shared -o libtest.so

// prerequisites: create a ramrole, grant it full access to OSS, then bind it with an ECS instance.
// usage: -ocredlib=[credlib_path] -ocredlib_opts=[ramrole]

#include <cstring>
#include <cstdio>
#include <malloc.h>
#include <curl/curl.h>
#include <sstream> 
#include <string>
#include <time.h>

extern "C" const char* VersionS3fsCredential(bool detail);
extern "C" bool InitS3fsCredential(const char* popts, char** pperrstr);
extern "C" bool FreeS3fsCredential(char** pperrstr);
extern "C" bool UpdateS3fsCredential(char** ppaccess_key_id, char** ppserect_access_key, char** ppaccess_token, long long* ptoken_expire, char** pperrstr);

static std::string ramrole;

// 2023-08-09T06:01:13Z
// this function translates an ISO 8601 time str to timestamp 
time_t string_to_time(const char *s) {
  struct tm tm;

  if (!s) {
    return 0L;
  }

  std::memset(&tm, 0, sizeof(struct tm));

  if (NULL == strptime(s, "%Y-%m-%dT%H:%M:%S", &tm)) {
    return 0L;
  }

  // timegm: convert UTC tm to timestamp regardless of localtime
  return timegm(&tm);
}

void parse_key(const std::string &line, std::string &val) {
  size_t last_pos = line.find_last_of("\"");
  size_t second_last_pos = line.substr(0, last_pos).find_last_of("\"");
  val = line.substr(second_last_pos + 1, last_pos - second_last_pos - 1);
}

void parse_token_info(const std::string &userdata, 
                      std::string &ak, 
                      std::string &sk, 
                      std::string &token, 
                      std::string &expiration) {
  // userdata example: 
  //  {
  //   "AccessKeyId" : "STS.xxx",
  //   "AccessKeySecret" : "xxx",
  //   "Expiration" : "2024-09-04T14:22:13Z",
  //   "SecurityToken" : "xxx",
  //   "LastUpdated" : "2024-09-04T08:22:13Z",
  //   "Code" : "Success"
  // }

  std::string key1 = "AccessKeyId", key2 = "AccessKeySecret", key3 = "SecurityToken", key4 = "Expiration";
  std::istringstream iss(userdata);
  std::string item;
  while (std::getline(iss, item)) {
    size_t pos_1 = item.find_first_of("\"");
    if (pos_1 == std::string::npos) continue;

    if (item.find(key1) != std::string::npos) {
      parse_key(item, ak);
    } else if (item.find(key2) != std::string::npos) {
      parse_key(item, sk);
    } else if (item.find(key3) != std::string::npos) {
      parse_key(item, token);
    } else if (item.find(key4) != std::string::npos) {
      parse_key(item, expiration);
    }
  }
}

size_t responseStr(void* ptr, size_t size, size_t nmemb, void *userdata) {
  if (ptr == NULL || userdata == NULL || size == 0) {
      return 0;
  }
  size_t realSize = size*nmemb;
  std::string *str = (std::string*)userdata;
  (*str).append((char*)ptr, realSize);

  return realSize;
}

// Get current instance user data
int get_ecs_userdata(std::string &ak, 
                     std::string &sk, 
                     std::string &token, 
                     std::string &expiration) {
  // ossfs has already done curl_global_init
  CURL* curl = curl_easy_init();
  if (!curl) {
    return -1;
  }

  std::string root_url = "http://100.100.100.200/latest/meta-data/ram/security-credentials/";
  std::string url = root_url + ramrole;
  std::string data;

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, responseStr);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)&data);

  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    curl_easy_cleanup(curl);
    return -1;
  }

  parse_token_info(data, ak, sk, token, expiration);
  curl_easy_cleanup(curl);
  return 0;
}

const char* VersionS3fsCredential(bool detail) {
  return "0.0.0";
}

bool InitS3fsCredential(const char* popts, char** pperrstr) {
  if(popts && 0 < strlen(popts)){
    ramrole = std::string(popts);
  }

  if(pperrstr){
    *pperrstr = strdup("The external credential library does not have InitS3fsCredential function, so built-in function was called.");
  }

  return true;
}

bool FreeS3fsCredential(char** pperrstr) {
  return true;
}

bool UpdateS3fsCredential(char** ppaccess_key_id, char** ppserect_access_key, char** ppaccess_token, long long* ptoken_expire, char** pperrstr) {
  std::string akstr, skstr, tokenstr, expstr;
  get_ecs_userdata(akstr, skstr, tokenstr, expstr);

  char *ak = (char *)malloc(akstr.size()+1);
  memset(ak, 0, akstr.size()+1);
  strcpy(ak, akstr.c_str());

  char *sk = (char *)malloc(skstr.size()+1);
  memset(sk, 0, skstr.size()+1);
  strcpy(sk, skstr.c_str());

  char *token = (char *)malloc(tokenstr.size()+1);
  memset(token, 0, tokenstr.size()+1);
  strcpy(token, tokenstr.c_str());

  if(ppaccess_key_id){
    *ppaccess_key_id = ak;
  }
  if(ppserect_access_key){
    *ppserect_access_key = sk;
  }
  if(ppaccess_token){
    *ppaccess_token = token;
  }
  if (ptoken_expire) {
    *ptoken_expire = string_to_time(expstr.c_str());
  }
  return true;
}