/*
 * ossfs - FUSE-based file system backed by Alibaba Cloud OSS
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

#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <limits>
#include <stdint.h>
#include <cstring>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>
#include <vector>

#include "threadpoolman.h"
#include "test_util.h"

void* simple_worker(void* arg) {
    std::atomic<int> *counter = static_cast<std::atomic<int>*>(arg);
    (*counter)++;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return nullptr;
}

void* error_worker(void* arg) {
    std::atomic<int>* counter = static_cast<std::atomic<int>*>(arg);
    (*counter)++;
    return reinterpret_cast<void*>(-1); // Simulate an error by returning non-null value
}

// Worker function that increments a shared counter with a semaphore
struct SemaphoreData {
    std::atomic<int>* counter;
    Semaphore* sem;
};
void* semaphore_worker(void* arg) {
    SemaphoreData* data = static_cast<SemaphoreData*>(arg);
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Simulate work
    (*data->counter)++;
    data->sem->post();
    return nullptr;
}

void test_threadpoolman_basic() {
    int thread_count = 5;
    ASSERT_TRUE(ThreadPoolMan::Initialize(thread_count));
    std::cout << "Thread pool initialized with " << thread_count << " threads."
              << std::endl;

    // Case1: test if there is only a single instance of ThreadPoolMan
    ASSERT_TRUE(ThreadPoolMan::Initialize(thread_count));
    ThreadPoolMan::Destroy();

    std::atomic<int> counter{0};
    thpoolman_param* param1 = new thpoolman_param();
    param1->args = &counter;
    param1->pfunc = simple_worker;
    ASSERT_FALSE(ThreadPoolMan::Instruct(param1));

    // Case2: test destory no existing thread pool
    ASSERT_TRUE(ThreadPoolMan::Initialize(thread_count));
    ThreadPoolMan::Destroy();
    ThreadPoolMan::Destroy();

    thpoolman_param* param2 = new thpoolman_param();
    param2->args = &counter;
    param2->pfunc = simple_worker;
    ASSERT_FALSE(ThreadPoolMan::Instruct(param2));

    ASSERT_TRUE(ThreadPoolMan::Initialize(thread_count));
    ASSERT_TRUE(ThreadPoolMan::Instruct(param2));

    ThreadPoolMan::Destroy();

    // Case3: test Initialize already exists thread pool
    ASSERT_TRUE(ThreadPoolMan::Initialize(thread_count));

    int new_thread_count = thread_count * 3;
    ASSERT_TRUE(ThreadPoolMan::Initialize(new_thread_count));
    std::cout << "Thread pool re-create to " << new_thread_count << " threads."
              << std::endl;

    ThreadPoolMan::Destroy();

    // Case4: test parameters for worker threads
    ASSERT_TRUE(ThreadPoolMan::Initialize(thread_count));

    counter = 0;
    for (int i = 0; i < thread_count; ++i) {
        thpoolman_param* param = new thpoolman_param();
        param->args = &counter;
        param->pfunc = simple_worker;
        ASSERT_TRUE(ThreadPoolMan::Instruct(param));
    }

    std::this_thread::sleep_for(std::chrono::seconds(3));
    ASSERT_EQUALS(counter, thread_count);
    std::cout << "All " << thread_count << " workers have been executed." << std::endl;

    ThreadPoolMan::Destroy();

    // case5: test error handling
    ASSERT_TRUE(ThreadPoolMan::Initialize(5));
    std::cout << "Thread pool initialized for error handling test."
              << std::endl;

    counter = 0;
    thpoolman_param* param = new thpoolman_param();
    param->args = &counter;
    param->pfunc = error_worker;
    ASSERT_TRUE(ThreadPoolMan::Instruct(param));

    std::this_thread::sleep_for(
        std::chrono::seconds(1));  // Wait for task to complete

    ASSERT_EQUALS(counter, 1);

    // case6: test null param
    param = nullptr;
    ASSERT_FALSE(ThreadPoolMan::Instruct(param));

    ThreadPoolMan::Destroy();
}

void test_threadpoolman_high_concurrency() {
  int thread_count = 100;
  ASSERT_TRUE(ThreadPoolMan::Initialize(thread_count));
  std::cout << "Thread pool initialized with " << thread_count << " threads."
            << std::endl;

  // Create parameters for worker threads
  std::atomic<int> counter{0};
  Semaphore sem(0);
  const int num_instructions = 1000;
  for (int i = 0; i < num_instructions; ++i) {
    thpoolman_param* param = new thpoolman_param();
    SemaphoreData* data = new SemaphoreData{&counter, &sem};
    param->args = data;
    param->pfunc = semaphore_worker;
    ASSERT_TRUE(ThreadPoolMan::Instruct(param));
  }

  // Wait for all threads to finish using semaphore
  for (int i = 0; i < num_instructions; ++i) {
    sem.wait();
  }

  ASSERT_EQUALS(counter, num_instructions);
  std::cout << "All " << num_instructions << " workers have been executed."
            << std::endl;

  ThreadPoolMan::Destroy();
  std::cout << "Thread pool destroyed." << std::endl;
}

int main(int argc, char *argv[]) {
    std::cout << "Running ThreadPoolMan Test" << std::endl;

    test_threadpoolman_basic();
    test_threadpoolman_high_concurrency();

    return 0;
}