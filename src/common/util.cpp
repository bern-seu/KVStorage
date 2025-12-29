#include "util.h"
#include <chrono>

std::chrono::_V2::system_clock::time_point now() { return std::chrono::high_resolution_clock::now(); }

void DPrintf(const char *format, ...){
    if(Debug){
        // 获取当前的日期，然后取日志信息，写入相应的日志文件当中 a+
        time_t now = time(nullptr);
        tm *nowtm;
        localtime_r(&now,nowtm);
        va_list args;
        va_start(args, format);
        std::printf("[%d-%d-%d-%d-%d-%d]", nowtm->tm_year + 1900, nowtm->tm_mon + 1, nowtm->tm_mday, nowtm->tm_hour,
                nowtm->tm_min, nowtm->tm_sec);
        std::vprintf(format, args);
        std::printf("\n");
        va_end(args);
    }
}

void myAssert(bool condition, std::string message) {
    if (!condition) {
        std::cerr << "Error: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

void sleepNMilliseconds(int N) { std::this_thread::sleep_for(std::chrono::milliseconds(N)); };

std::chrono::milliseconds getRandomizedElectionTimeout() {
    // 只在第一次运行初始化，之后一直这就这一个对象
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime);
    // 每次调用，rng 内部状态都会变，dist 就会映射出不同的结果
    return std::chrono::milliseconds(dist(rng));
}