#include <cstdlib>
#include <cstring>

#include "s3fs_logger.h"
#include "memorypool.h"

//------------------------------------------------
// MemoryPool class variables
//------------------------------------------------
MemoryPool* MemoryPool::memoryPool = nullptr;
std::mutex MemoryPool::memory_pool_lock;

//------------------------------------------------
// MemoryPool class methods
//------------------------------------------------
bool MemoryPool::Initialize(uint64_t max_free_capacity, size_t block_size) {
    if (MemoryPool::memoryPool == nullptr) {
        std::lock_guard<std::mutex> lock(MemoryPool::memory_pool_lock);
        if (MemoryPool::memoryPool == nullptr) {
            MemoryPool::memoryPool = new MemoryPool(max_free_capacity, block_size);
        }
    }
    return true;
}
 
void MemoryPool::Destroy() {
    if (MemoryPool::memoryPool != nullptr) {
        std::lock_guard<std::mutex> lock(MemoryPool::memory_pool_lock);
        if (MemoryPool::memoryPool != nullptr) {
            delete MemoryPool::memoryPool;
            MemoryPool::memoryPool = nullptr;
        }
    }
    S3FS_PRN_INFO("MemoryPool destroyed.");
}

//------------------------------------------------
// MemoryPool methods
//------------------------------------------------
MemoryPool::MemoryPool(uint64_t tmax_free_capacity, size_t tblock_size)
    : max_free_capacity(tmax_free_capacity), block_size(tblock_size) {}

MemoryPool::~MemoryPool() {
    for (char* ptr : freeList) {
        free(ptr);
    }
}

char* MemoryPool::Allocate() {
    std::lock_guard<std::mutex> lock(free_list_lock);
    if (freeList.empty()) {
        char* ptr = ExpandOne();
        return ptr;
    }
    char* ptr = freeList.back();
    freeList.pop_back();
    return ptr;
}

void MemoryPool::Deallocate(char* ptr) {
    std::lock_guard<std::mutex> lock(free_list_lock);
    if (freeList.size() < max_free_capacity) {
        freeList.push_back(ptr);
    } else {
        free(ptr);
    }
}

char* MemoryPool::ExpandOne() {
    char* ptr = nullptr;
    int r = posix_memalign(reinterpret_cast<void**>(&ptr), 4096, block_size);
    if (r != 0) {
        S3FS_PRN_ERR("failed to allocate memory with return code %d", r);
        return nullptr;
    }
    return ptr;
}