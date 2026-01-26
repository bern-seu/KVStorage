#include <iostream>
#include <chrono>
#include <vector>
#include <thread>
#include <atomic>
#include "clerk.h"

// 压测配置
const int THREAD_NUM = 50;    // 开 10 个线程模拟并发客户端
const int REQUESTS_PER_THREAD = 200; // 每个线程发 2000 个请求

void benchmark_task(int thread_id) {
    Clerk client;
    client.Init("test.conf"); // 每个线程甚至可以有独立的 Client 连接
    for (int i = 0; i < REQUESTS_PER_THREAD; ++i) {
        // Key 带上线程 ID，避免所有线程都在抢同一个 Key (Raft 状态机冲突)
        std::string key = "k_" + std::to_string(thread_id) + "_" + std::to_string(i);
        client.Put(key, "bench_value");
    }
}

int main() {
    std::cout << "Starting Multi-threaded Benchmark..." << std::endl;
    std::cout << "Threads: " << THREAD_NUM << ", Req/Thread: " << REQUESTS_PER_THREAD << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < THREAD_NUM; ++i) {
        threads.emplace_back(benchmark_task, i);
    }

    // 等待所有线程跑完
    for (auto& t : threads) {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    
    std::chrono::duration<double> duration = end - start;
    double total_requests = THREAD_NUM * REQUESTS_PER_THREAD;
    double qps = total_requests / duration.count();

    std::cout << "Done! QPS: " << qps << " Op/s" << std::endl;

    return 0;
}