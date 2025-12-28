#ifndef UTIL_H
#define UTIL_H
#include <string>
#include <sstream>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <iostream>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>

#include "config.h"

template <class F>
class DeferClass{
public:
    DeferClass(F&& f) : m_func(std::forward<F>(f)) {}
    DeferClass(const F& f) : m_func(f) {}
    ~DeferClass() {m_func();}

    DeferClass(const DeferClass& e) = delete;
    DeferClass& operator=(const DeferClass&e) = delete;
private:
    F m_func;
};

#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) DeferClass _CONCAT(defer_placeholder, line) = [&]()

#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)


void DPrintf(const char* format, ...);
void myAssert(bool condition, std::string message = "Assertion failed!");
std::chrono::_V2::system_clock::time_point now();

template <typename... Args>
std::string format(const char* format_str, Args... args){
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1; // "\0"
    if (size_s <= 0) { throw std::runtime_error("Error during formatting."); }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1);  // remove '\0'
}

//异步队列
template<class T>
class LockQueue{
private:
    //int m_capacity;
    std::mutex m_mutex;
    std::queue<T> m_queue;
    std::condition_variable m_condvariable;
    //std::condition_variable m_conCustomer;
public:
    // 多个worker线程都会写日志queue
    void Push(const T& data) {
        std::lock_guard<std::mutex> lock(m_mutex);  //使用lock_gurad，即RAII的思想保证锁正确释放
        m_queue.push(data);
        m_condvariable.notify_one();
    }
        // 一个线程读日志queue，写日志文件
    T Pop() {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_queue.empty()) {
            // 日志队列为空，线程进入wait状态
            m_condvariable.wait(lock);  //这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持
        }
        T data = m_queue.front();
        m_queue.pop();
        return data;
    }
    bool timeOutPop(int timeout, T* ResData)    // 添加一个超时时间参数，默认为 50 毫秒
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        // 获取当前时间点，并计算出超时时刻
        auto now = std::chrono::system_clock::now();
        auto timeout_time = now + std::chrono::milliseconds(timeout);

        // 在超时之前，不断检查队列是否为空
        while(m_queue.empty()){
            // 如果已经超时了，就返回一个空对象
            if(m_condvariable.wait_until(lock,timeout_time) == std::cv_status::timeout){
                return false;
            }else{
                continue;
            }
        }
        T data = m_queue.front();
        m_queue.pop();
        *ResData = data;
        return true;
    }
};
//这个Op是kvserver传递给raft的command
class Op
{
private:
    std::string Operation;  // "Get" "Put" "Append"
    std::string Key;
    std::string Value;
    std::string ClientId;  //客户端号码
    int RequestId;         //客户端号码请求的Request的序列号，为了保证线性一致性

public:
    //TODO 为了协调raftRPC中的command只设置成了string,这个的限制就是正常字符中不能包含|,后期可以换成更高级的序列化方法，比如protobuf
    std::string asString() const{
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        // write class instance to archive
        oa << *this;
        // close archive
        return ss.str();
    }
    bool parseFromString(std::string str){
        std::stringstream iss(str);
        boost::archive::text_iarchive ia(iss);
        // read class state from archive
        ia >> *this;
        return true;  // todo : 解析失敗如何處理，要看一下boost庫了
    }
public:
    friend std::ostream& operator<<(std::ostream& os, const Op& obj){
        os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
              obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "}";  // 在这里实现自定义的输出格式
        return os;
    }
private:
    friend class boost::serialization::access;//允许Boost访问私有成员
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        // 注意这里的 '&' 符号，它是 Boost 定义的一个特殊操作符
        ar & Operation;
        ar & Key;
        ar & Value;
        ar & ClientId;
        ar & RequestId;
    }
};


#endif  //  UTIL_H