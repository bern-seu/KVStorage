/* Raft低层的接口框架 */
#ifndef RAFT_H
#define RAFT_H

#include "raftRPC.pb.h"
#include "ApplyMsg.h"
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
public:
    //AppendEntries方法的本地实现(利用了重载)
    void AppendEntries(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);
    //定期向状态机写入日志
    void applierTicker();
    //记录某个时刻的状态
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

private:

};

#endif  // RAFT_H