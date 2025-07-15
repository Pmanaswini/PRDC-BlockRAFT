#ifndef BLOCK_SHARED_H
#define BLOCK_SHARED_H

#include <atomic>
#include <mutex>
#include <queue>

extern std::queue<int> blockQueue;
extern std::mutex queueMutex;
extern std::atomic<bool> received_block;

#endif  // BLOCK_SHARED_H
