#include "Persister.h"
#include "util.h" 
#include <vector>
#include <cstdio> // for rename

Persister::Persister(int me)
    : m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt") 
{
    // 构造时，不进行覆盖写，这样节点宕机重启后，仍然能恢复数据
}

Persister::~Persister() {
    // 局部变量流会自动析构
}

void Persister::Save(const std::string& raftstate, const std::string& snapshot) {
    std::lock_guard<std::mutex> lg(m_mtx);
    
    // 1. 保存 RaftState
    // todo:使用临时文件 + rename 保证原子性（简易版可以直接覆盖写，但必须用二进制模式）, 不使用临时文件，如果写一半崩溃了，磁盘上的文件为空或者损坏了，重启节点后数据会丢失
    std::ofstream outfile(m_raftStateFileName, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!outfile.is_open()) {
        DPrintf("[Persister] Save raftstate open error");
        return;
    }
    outfile.write(raftstate.c_str(), raftstate.size());
    outfile.close();

    // 2. 保存 Snapshot
    std::ofstream snapFile(m_snapshotFileName, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!snapFile.is_open()) {
        DPrintf("[Persister] Save snapshot open error");
        return;
    }
    snapFile.write(snapshot.c_str(), snapshot.size());
    snapFile.close();
}

void Persister::SaveRaftState(const std::string& data) {
    std::lock_guard<std::mutex> lg(m_mtx);
    
    // 打开文件，使用 Trunc 模式覆盖旧内容，使用 Binary 模式防止换行符转换
    std::ofstream outfile(m_raftStateFileName, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!outfile.is_open()) {
        DPrintf("[Persister] SaveRaftState open error");
        return;
    }
    outfile.write(data.c_str(), data.size());
    outfile.close();
    m_raftStateSize = data.size();
}

long long Persister::RaftStateSize() {
    std::lock_guard<std::mutex> lg(m_mtx);
    
    // std::ifstream infile(m_raftStateFileName, std::ios::ate | std::ios::binary); // ate: 指针定位到文件末尾
    // if (!infile.is_open()) {
    //     return 0;
    // }
    // return infile.tellg(); // 返回当前文件读取指针距离文件开头的字节偏移量
    return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
    std::lock_guard<std::mutex> lg(m_mtx);
    
    std::ifstream infile(m_raftStateFileName, std::ios::in | std::ios::binary);
    if (!infile.is_open()) {
        return "";
    }
    
    //读取整个文件内容到 string，而不是用 >>
    // 获取文件大小
    infile.seekg(0, std::ios::end);//把读取指针从文件末尾开始，偏移 0 个字节
    size_t size = infile.tellg();
    if (size == 0) return "";
    
    std::string data;
    data.resize(size);
    
    infile.seekg(0, std::ios::beg);//把读取指针从文件开头开始，偏移 0 个字节
    infile.read(&data[0], size);
    infile.close();
    
    return data;
}

std::string Persister::ReadSnapshot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    
    std::ifstream infile(m_snapshotFileName, std::ios::in | std::ios::binary);
    if (!infile.is_open()) {
        return "";
    }
    
    // 同上，读取二进制全量数据
    infile.seekg(0, std::ios::end);
    size_t size = infile.tellg();
    if (size == 0) return "";
    
    std::string data;
    data.resize(size);
    
    infile.seekg(0, std::ios::beg);
    infile.read(&data[0], size);
    infile.close();
    
    return data;
}