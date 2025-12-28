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