/* Raft低层的接口框架 */
#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <mutex>
#include <cmath>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <iostream>

#include "boost/serialization/serialization.hpp"
#include "boost/any.hpp"

#include "Persister.h"
#include "raftRPC.pb.h"
#include "ApplyMsg.h"
#include "util.h"
#include "raftRpcUtil.h"
#include "monsoon.h"
/* 定义一些常量表达式 */
/* 网络状态 */
//网络异常的时候为Disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int Disconnected = 0;
constexpr int AppNormal = 1;

/* 投票状态 */
constexpr int Killed = 0;
constexpr int Voted = 1; //本轮已经投过票了
constexpr int Expire = 2; //投票（消息、竞选者）过期
constexpr int Normal = 3;

//基于通信协议，提供raft节点的rpc服务
class Raft : public raftRpcProctoc::raftRpc{
private:
    std::mutex m_mtx;
    //m_peers 是 Raft 逻辑层通往外部世界的“唯一网关”
    std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
    std::shared_ptr<Persister> m_persister;
    int m_me;
    int m_currentTerm;
    int m_votedFor;
    std::vector<raftRpcProctoc::LogEntry> m_logs; //日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号.这两个状态所有结点都在维护，易失
    // 已经汇报给状态机（上层应用）的log 的index
    int m_commitIndex;
    int m_lastApplied;
    // 这两个状态是由服务器来维护，易失
    // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
    std::vector<int> m_nextIndex;
    std::vector<int> m_matchIndex;
    enum Status { Follower, Candidate, Leader};
    // 身份
    std::atomic<Status> m_status;
    // raft内部使用的chan，applyChan是用于和服务层交互,最后好像没用上
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan;
    // 选举超时
    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
    // 心跳超时，用于leader
    std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;
    // 2D中用于传入快照点
    // 储存了快照中的最后一个日志的Index和Term
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    // 协程
    std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

public:
    //AppendEntries方法的本地实现(利用了重载)
    void AppendEntries(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);
    //定期向状态机写入日志
    void applierTicker();
    //用于确认并真正安装一个从 Leader 那里接收到的快照，确保在应用安装快照的这段时间窗口内，Raft 状态机没有因为处理了新的日志而导致这个快照变得“过时”
    bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);
    //发起选举,心跳超时，Follower成为candidate，term+1，之后发起选举
    void doElection();
    //发起心跳，只有leader才需要发起心跳
    void doHeartBeat();
     /* 
     每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了
     如果有则设置合适睡眠时间：睡眠到重置时间+超时时间 
     */
    //监控是否发起选举
    void electionTimeOutTicker();
    //获取应用日志，KVSever（上层应用）主动向 Raft（底层共识层）索取“已经达成共识、可以执行”的日志
    std::vector<ApplyMsg> getApplyLogs();
    //获取新命令的索引（Client 发给 Leader、Leader 自己产生的命令）
    int getNewCommandIndex();
    //获取当前的日志信息
    void getPrevLogInfo(int server, int *preIndex, int *preTerm);
    //看当前节点是否是领导节点
    void GetState(int *term, bool *isLeader);
    //InstallSnapshot方法的本地实现,安装快照
    void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                       raftRpcProctoc::InstallSnapshotResponse *reply);
    //负责查看是否该发送心跳了，如果该发起就执行doHeartBeat()
    void leaderHearBeatTicker();
    //Leader 发送快照
    void leaderSendSnapShot(int server);
    //检查是否有某条日志已经被复制到了“大多数”节点上。如果是，Leader 就会把这条日志标记为“已提交（Committed）”
    void leaderUpdateCommitIndex();
    //对象index日志是否匹配，Follower 用来检查自己的日志，跟 Leader 发来的日志是否“接得上”
    bool matchLog(int logIndex, int logTerm);
    //持久化当前状态，当前的任期，当前任期投给了谁，日志条目数组
    void persist();
    //请求投票的处理（重载）
    void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);
    //判断当前节点是否有最新日志
    bool UpToDate(int index, int term);
    //获取最后一个日志entry的term，index
    int getLastLogIndex();
    int getLastLogTerm();
    //获取最后一个日志entry的term和index
    void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
    //获取指定日志索引的term
    int getLogTermFromLogIndex(int logIndex);
    //获取Raft状态的大小，目前持久化存储的数据所占用的字节数
    /* 
    Raft 现在的日志文件有多大了？这个函数的存在目的只有一个：为了触发快照
    KVServer会定期调用GetRaftStateSize() 来监控 Raft，如果日志太大了，就会制作快照
    发送快照的场景：
    一、KVServer -> 本地 Raft；二、Leader -> 落后的 Follower
     */
    int GetRaftStateSize();
    //将日志索引转换为日志entry在m_logs数组中的位置
    int getSlicesIndexFromLogIndex(int logIndex);
    //请求其他节点给自己投票，
    bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                        std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<std::atomic<int>> votedNum);
    //发送追加日志条目
    bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                            std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<std::atomic<int>> appendNums);

    // 给上层的KVserver发送消息，rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread，避免使用pthread_create，因此专门写一个函数来执行
    void pushMsgToKvServer(ApplyMsg msg);
    //读取持久化数据,将从硬盘里读出来的二进制字符串，还原成 Raft 节点的内存变量。（反序列化）
    void readPersist(std::string data);
    //持久化数据,将 Raft 节点当前的 核心状态（Hard State） 转换成一个二进制字符串(序列化)
    std::string persistData();

    //整个 Raft 系统启动共识流程的唯一入口,是 KVServer 叫 Raft “干活” 的入口（提交新任务）
    void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

    // Snapshot the service says it has created a snapshot that has
    // all info up to and including index. this means the
    // service no longer needs the log through (and including)
    // that index. Raft should now trim its log as much as possible.
    // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
    // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
    // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
    void Snapshot(int index, std::string snapshot);

public:
    // 重写基类方法,因为rpc远程调用真正调用的是这个方法
    // 序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。 
    void AppendEntries(google::protobuf::RpcController* controller,
                        const ::raftRpcProctoc::AppendEntriesArgs* request,
                        ::raftRpcProctoc::AppendEntriesReply* response,
                        ::google::protobuf::Closure* done) override;
    void InstallSnapshot(google::protobuf::RpcController* controller,
                        const ::raftRpcProctoc::InstallSnapshotRequest* request,
                        ::raftRpcProctoc::InstallSnapshotResponse* response,
                        ::google::protobuf::Closure* done) override;
    void RequestVote(google::protobuf::RpcController* controller,
                        const ::raftRpcProctoc::RequestVoteArgs* request,
                        ::raftRpcProctoc::RequestVoteReply* response,
                        ::google::protobuf::Closure* done) override;
    void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);
private:
    // for persist
    class BoostPersistRaftNode{
        public:
            friend class boost::serialization::access;
            template <class Archive>
            void serialize(Archive &ar, const unsigned int version) {
                ar &m_currentTerm;
                ar &m_votedFor;
                ar &m_lastSnapshotIncludeIndex;
                ar &m_lastSnapshotIncludeTerm;
                ar &m_logs;
            }
            int m_currentTerm;
            int m_votedFor;
            int m_lastSnapshotIncludeIndex;
            int m_lastSnapshotIncludeTerm;
            std::vector<std::string> m_logs;
            std::unordered_map<std::string, int> umap;
    };
};

#endif  // RAFT_H