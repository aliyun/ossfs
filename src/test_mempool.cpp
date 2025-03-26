// memorypool_test.cpp
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

#include "memorypool.h"
#include "test_util.h"

void test_memorypool_basic() {
    size_t block_size = 1 * 1024 * 1024; // 1MB block size
    size_t max_free_capacity = 10; // 10 blocks in free list

    // Initialize memory pool
    ASSERT_TRUE(MemoryPool::Initialize(max_free_capacity, block_size));
    std::cout << "Memory pool initialized with block size " << block_size << " bytes and max free capacity " << max_free_capacity << " blocks." << std::endl;

    // Allocate memory blocks
    std::vector<char*> allocated_blocks;
    for (size_t i = 0; i < max_free_capacity; ++ i) {
        char* block = MemoryPool::memoryPool->Allocate();
        if (block) {
            allocated_blocks.push_back(block);
            memset(block, 0, block_size);
        } else {
            std::cout << "Failed to allocate block " << i << std::endl;
            break;
        }
    }
    ASSERT_EQUALS(allocated_blocks.size(), (size_t)max_free_capacity);
    std::cout << "Allocated " << allocated_blocks.size() << " blocks." << std::endl;

    // Deallocate some memory blocks, still in free list
    size_t deallocate_count = 5;
    allocated_blocks.erase(allocated_blocks.begin(), allocated_blocks.begin() + deallocate_count);
    std::cout << "Deallocated " << deallocate_count << " blocks." << std::endl;

    // Allocate more memory blocks
    for (size_t i = 0; i < deallocate_count; ++ i) {
        char* block = MemoryPool::memoryPool->Allocate();
        if (block) {
            allocated_blocks.push_back(block);
            memset(block, 0, block_size);
        } else {
            std::cout << "Failed to allocate block " << i << std::endl;
            break;
        }
    }
    ASSERT_EQUALS(allocated_blocks.size(), (size_t)max_free_capacity);

    // Deallocate all memory blocks
    for (char* block : allocated_blocks) {
        MemoryPool::memoryPool->Deallocate(block);
    }
    allocated_blocks.clear();
    std::cout << "Deallocated all blocks." << std::endl;

    // Destroy memory pool
    MemoryPool::Destroy();
    std::cout << "Memory pool destroyed." << std::endl;
}

void test_memorypool_concurrent() {
    size_t block_size = 1 * 1024 * 1024; // 1MB block size
    size_t max_free_capacity = 10; // 10 blocks in free list

    // Initialize memory pool
    ASSERT_TRUE(MemoryPool::Initialize(max_free_capacity, block_size));
    std::cout << "Memory pool initialized with block size " << block_size << " bytes and max free capacity " << max_free_capacity << " blocks." << std::endl;

    // Create multiple threads to allocate and deallocate memory
    const size_t num_threads = 5;
    const size_t allocations_per_thread = 10;

    std::vector<std::thread> threads;
    std::vector<std::vector<char*>> thread_allocated_blocks(num_threads);

    for (size_t i = 0; i < num_threads; ++ i) {
        threads.emplace_back([i, &thread_allocated_blocks, block_size]() {
            for (size_t j = 0; j < allocations_per_thread; ++ j) {
                char* block = MemoryPool::memoryPool->Allocate();
                if (block) {
                    thread_allocated_blocks[i].push_back(block);
                    memset(block, 0, block_size);
                } else {
                    std::cout << "Thread " << i << " failed to allocate block " << j << std::endl;
                    break;
                }
            }
            std::cout << "Thread " << i << " allocated " << thread_allocated_blocks[i].size() << " blocks." << std::endl;
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::cout << "All threads finished allocation." << std::endl;

    // Deallocate memory blocks in each thread
    threads.clear();
    for (size_t i = 0; i < num_threads; ++ i) {
        threads.emplace_back([i, &thread_allocated_blocks]() {
            for (char* block : thread_allocated_blocks[i]) {
                MemoryPool::memoryPool->Deallocate(block);
            }
            std::cout << "Thread " << i << " deallocated all blocks." << std::endl;
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::cout << "All threads finished deallocation." << std::endl;

    // Destroy memory pool
    MemoryPool::Destroy();
    std::cout << "Memory pool destroyed." << std::endl;
}

void test_memorypool_expand() {
    size_t block_size = 1 * 1024 * 1024; // 1MB block size
    size_t max_free_capacity = 10; // 10 blocks in free list

    // Initialize memory pool
    ASSERT_TRUE(MemoryPool::Initialize(max_free_capacity, block_size));
    std::cout << "Memory pool initialized with block size " << block_size << " bytes and max free capacity " << max_free_capacity << " blocks." << std::endl;

    // Allocate memory blocks to fill the free list
    std::vector<char*> allocated_blocks;
    for (size_t i = 0; i < max_free_capacity; ++i) {
        char* block = MemoryPool::memoryPool->Allocate();
        if (block) {
            allocated_blocks.push_back(block);
            memset(block, 0, block_size);
        } else {
            std::cout << "Failed to allocate block " << i << std::endl;
            break;
        }
    }
    ASSERT_EQUALS(allocated_blocks.size(), max_free_capacity);
    std::cout << "Allocated " << allocated_blocks.size() << " blocks." << std::endl;

    // Deallocate one block
    MemoryPool::memoryPool->Deallocate(allocated_blocks.back());
    char* ptr = allocated_blocks.back();
    allocated_blocks.pop_back();

    std::cout << "Deallocated one block." << std::endl;

    // Allocate a new block
    char* extra_block = MemoryPool::memoryPool->Allocate();
    ASSERT_EQUALS(extra_block, ptr);
    allocated_blocks.push_back(extra_block);
    std::cout << "Allocated a new block." << std::endl;

    // Destroy memory pool
    MemoryPool::Destroy();
    std::cout << "Memory pool destroyed." << std::endl;
}

int main(int argc, char *argv[]) {
    std::cout << "Running MemoryPool Test" << std::endl;

    test_memorypool_basic();

    test_memorypool_concurrent();

    test_memorypool_expand();

    return 0;
}