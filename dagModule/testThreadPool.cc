#include <gtest/gtest.h>

#include "threadPool.h"  // Include the corrected ThreadPool implementation

TEST(ThreadPoolTest, TaskExecution) {
  ThreadPool pool(4);  // Create a thread pool with 4 threads

  // Shared variable to track task completion
  atomic<int> counter = 0;
  int num_tasks = 10;

  // Enqueue multiple tasks
  for (int i = 0; i < num_tasks; ++i) {
    pool.enqueue([&counter] {
      this_thread::sleep_for(chrono::milliseconds(100));  // Simulate work
      counter.fetch_add(1, memory_order_relaxed);
    });
  }

  // Wait for tasks to complete
  this_thread::sleep_for(chrono::seconds(2));

  // Verify all tasks were executed
  EXPECT_EQ(counter.load(memory_order_relaxed), num_tasks);
}

TEST(ThreadPoolTest, GracefulShutdown) {
  ThreadPool pool(2);

  // Enqueue a task that takes some time
  atomic<bool> task_executed = false;
  pool.enqueue([&task_executed] {
    this_thread::sleep_for(chrono::milliseconds(500));
    task_executed.store(true, memory_order_relaxed);
  });

  // Destructor should wait for the task to complete
  // before joining threads
  this_thread::sleep_for(chrono::milliseconds(100));  // Ensure the task starts
  EXPECT_FALSE(task_executed.load(
      memory_order_relaxed));  // Task should not be finished yet
}
