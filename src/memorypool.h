#ifndef S3FS_MEMORYPOOL_H_
#define S3FS_MEMORYPOOL_H_

#include <vector>
#include <mutex>
#include <atomic>

class MemoryPool {
public:
    static MemoryPool* memoryPool;

    static bool Initialize(uint64_t max_free_capacity, size_t block_size);

    static void Destroy();

    char* Allocate();

    void Deallocate(char* ptr);

private:
    MemoryPool(uint64_t tmax_free_capacity, size_t tblock_size);

    ~MemoryPool();

    char* ExpandOne();

    std::vector<char*> freeList;
    std::mutex free_list_lock;
    const uint64_t max_free_capacity = 256;       // Maximum number of free blocks, default is 256             
    size_t block_size = 4 * 1024 * 1024;          // Size of each block, default is 4MB

    static std::mutex memory_pool_lock;
};

#endif