/*
 * ossfs - FUSE-based file system backed by Alibaba Cloud OSS
 *
 * Copyright(C) 2020 Andrew Gaul <andrew@gaul.org>
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

#include <string>
#include <cstring>

#include "curl_util.h"
#include "test_util.h"

//---------------------------------------------------------
// S3fsCred Stub
//
// [NOTE]
// This test program links curl_util.cpp just to use the
// curl_slist_sort_insert function.
// This file has a call to S3fsCred::GetBucket(), which
// results in a link error. That method is not used in
// this test file, so define a stub class. Linking all
// implementation of the S3fsCred class or making all
// stubs is not practical, so this is the best answer.
//
class S3fsCred
{
    private:
        static std::string  bucket_name;
    public:
        static const std::string& GetBucket();
};

std::string  S3fsCred::bucket_name;

const std::string& S3fsCred::GetBucket()
{
	return S3fsCred::bucket_name;
}
//---------------------------------------------------------

#define ASSERT_IS_SORTED(x) assert_is_sorted((x), __FILE__, __LINE__)

void assert_is_sorted(struct curl_slist* list, const char *file, int line)
{
    for(; list != NULL; list = list->next){
        std::string key1 = list->data;
        key1.erase(key1.find(':'));
        std::string key2 = list->data;
        key2.erase(key2.find(':'));
        std::cerr << "key1: " << key1 << " key2: " << key2 << std::endl;

        if(strcasecmp(key1.c_str(), key2.c_str()) > 0){
            std::cerr << "not sorted: " << key1 << " " << key2 << " at " << file << ":" << line << std::endl;
            std::exit(1);
        }
    }
    std::cerr << std::endl;
}

size_t curl_slist_length(const struct curl_slist* list)
{
    size_t len = 0;
    for(; list != NULL; list = list->next){
        ++len;
    }
    return len;
}

void test_sort_insert()
{
    struct curl_slist* list = NULL;
    ASSERT_IS_SORTED(list);
    // add to head
    list = curl_slist_sort_insert(list, "2", "val");
    ASSERT_IS_SORTED(list);
    // add to tail
    list = curl_slist_sort_insert(list, "4", "val");
    ASSERT_IS_SORTED(list);
    // add in between
    list = curl_slist_sort_insert(list, "3", "val");
    ASSERT_IS_SORTED(list);
    // add to head
    list = curl_slist_sort_insert(list, "1", "val");
    ASSERT_IS_SORTED(list);
    ASSERT_STREQUALS("1: val", list->data);
    // replace head
    list = curl_slist_sort_insert(list, "1", "val2");
    ASSERT_IS_SORTED(list);
    ASSERT_EQUALS(static_cast<size_t>(4), curl_slist_length(list));
    ASSERT_STREQUALS("1: val2", list->data);
    curl_slist_free_all(list);
}

void test_slist_remove()
{
    struct curl_slist* list = NULL;

    // remove no elements
    ASSERT_EQUALS(static_cast<size_t>(0), curl_slist_length(list));
    list = curl_slist_remove(list, "1");
    ASSERT_EQUALS(static_cast<size_t>(0), curl_slist_length(list));

    // remove only element
    list = NULL;
    list = curl_slist_sort_insert(list, "1", "val");
    ASSERT_EQUALS(static_cast<size_t>(1), curl_slist_length(list));
    list = curl_slist_remove(list, "1");
    ASSERT_EQUALS(static_cast<size_t>(0), curl_slist_length(list));

    // remove head element
    list = NULL;
    list = curl_slist_sort_insert(list, "1", "val");
    list = curl_slist_sort_insert(list, "2", "val");
    ASSERT_EQUALS(static_cast<size_t>(2), curl_slist_length(list));
    list = curl_slist_remove(list, "1");
    ASSERT_EQUALS(static_cast<size_t>(1), curl_slist_length(list));
    curl_slist_free_all(list);

    // remove tail element
    list = NULL;
    list = curl_slist_sort_insert(list, "1", "val");
    list = curl_slist_sort_insert(list, "2", "val");
    ASSERT_EQUALS(static_cast<size_t>(2), curl_slist_length(list));
    list = curl_slist_remove(list, "2");
    ASSERT_EQUALS(static_cast<size_t>(1), curl_slist_length(list));
    curl_slist_free_all(list);

    // remove middle element
    list = NULL;
    list = curl_slist_sort_insert(list, "1", "val");
    list = curl_slist_sort_insert(list, "2", "val");
    list = curl_slist_sort_insert(list, "3", "val");
    ASSERT_EQUALS(static_cast<size_t>(3), curl_slist_length(list));
    list = curl_slist_remove(list, "2");
    ASSERT_EQUALS(static_cast<size_t>(2), curl_slist_length(list));
    curl_slist_free_all(list);
}

void test_curl_slist_sort_insert() 
{
    struct curl_slist* list = NULL;
    ASSERT_IS_SORTED(list);
    
    list = curl_slist_sort_insert(list, "2:val2");
    ASSERT_IS_SORTED(list);
    ASSERT_STREQUALS("2: val2", list->data);
    
    list = curl_slist_sort_insert(list, "4:val4");
    ASSERT_IS_SORTED(list);
    ASSERT_STREQUALS("2: val2", list->data);
    
    list = curl_slist_sort_insert(list, "1:val1");
    ASSERT_IS_SORTED(list);
    ASSERT_STREQUALS("1: val1", list->data);
    
    list = curl_slist_sort_insert(list, "3:val3");
    ASSERT_IS_SORTED(list);
    ASSERT_STREQUALS("1: val1", list->data);
    
    ASSERT_EQUALS(static_cast<size_t>(4), curl_slist_length(list));

    // check all elements in list
    int i = 1;
    for(auto head = list; head != NULL; head = head->next, i ++) {
        std::string element = std::to_string(i) + ": val" + std::to_string(i);
        ASSERT_STREQUALS(element.c_str(), head->data);
    }
    curl_slist_free_all(list);
}    
void test_make_md5_from_binary() {
    std::string md5;

    // Normal case: Non-empty input
    const char* data = "Hello, World!";
    size_t length = strlen(data);
    ASSERT_TRUE(make_md5_from_binary(data, length, md5));

    // Invalid case: Empty input
    const char* empty_data = "";
    size_t empty_length = strlen(empty_data);
    ASSERT_FALSE(make_md5_from_binary(empty_data, empty_length, md5));

    // Invalid case: Null pointer
    ASSERT_FALSE(make_md5_from_binary(nullptr, 0, md5));
    
    // Invalid case: Empty string with non-null pointer
    const char* empty_string = "\0";
    ASSERT_FALSE(make_md5_from_binary(empty_string, 1, md5));
}

int main(int argc, char *argv[])
{
    test_sort_insert();
    test_slist_remove();
    test_curl_slist_sort_insert();
    test_make_md5_from_binary();
    return 0;
}

/*
* Local variables:
* tab-width: 4
* c-basic-offset: 4
* End:
* vim600: expandtab sw=4 ts=4 fdm=marker
* vim<600: expandtab sw=4 ts=4
*/
