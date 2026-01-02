#ifndef PERSISTER_H
#define PERSISTER_H

#include <fstream>
#include <mutex>
#include <string>

class Persister {
private:
    std::mutex m_mtx;
    std::string m_raftStateFileName;
    std::string m_snapshotFileName;
    long long m_raftStateSize = 0; // 缓存文件大小
    // 不需要维护成员变量的 fstream，随用随开更安全

public:
    explicit Persister(int me);
    ~Persister();

    void Save(const std::string& raftstate, const std::string& snapshot);
    std::string ReadSnapshot();
    void SaveRaftState(const std::string& data);
    long long RaftStateSize();
    std::string ReadRaftState();
};

#endif // PERSISTER_H