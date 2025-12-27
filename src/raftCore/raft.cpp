#include "raft.h"
#include "config.h"

//AppendEntries方法的本地实现(利用了重载)
void Raft::AppendEntries(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply)
{
    std::lock_guard<std::mutex> locker(m_mtx);
    reply->set_appstate(AppNormal);
    //	不同的节点收到AppendEntries的反应是不同的，要注意无论什么时候收到rpc请求和响应都要检查term
    if(args->term() < m_currentTerm){
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100);// 让领导人可以及时更新自己
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
        return;
    }
    //    Defer ec1([this]() -> void { this->persist(); });
    DEFER { persist(); };  //由于这个局部变量创建在锁之后，因此执行persist的时候应该也是拿到锁的.
    if(args->term() > m_currentTerm){
        // 三变 ,防止遗漏，无论什么时候都是三变
        m_currentTerm = args->term();
        m_votedFor = -1; // 这里设置成-1有意义，如果突然宕机然后上线理论上是可以投票的
        // 这里不返回，让改节点尝试接收日志
        // 如果是领导人和candidate突然转到Follower不用其他操作
        // 如果本来就是Follower，那么其term变化，相当于“不言自明”的换了追随的对象，因为原来的leader的term更小，是不会再接收其消息了
        // 这两种情况都可以去接受日志
    }
    
    m_status = Follower; //只要收到合法的AE，说明有leader，要变为follower
    // term相等，重置选举时间
    m_lastResetElectionTime = now();
    //未完成
}

void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh)
{
    //与其他节点沟通的rpc类
    m_peers = peers;
    //持久化类
    m_presister = persister;
    //标记自己，不能给自己发送rpc
    m_me = me;
    {
        std::lock_guard<std::mutex> locker(m_mtx);
        applyChan = applyCh;
        m_currentTerm = 0;
        m_status = Follower;
        m_commitIndex = 0;
        m_lastApplied = 0;
        m_logs.clear();
        for(int i = 0; i < m_peers.size(); i++){
            m_matchIndex.push_back(0);
            m_nextIndex.push_back(0);
        }
        m_votedFor = -1;
        m_lastSnapshotIncludeIndex = 0;
        m_lastSnapshotIncludeTerm = 0;
        m_lastResetElectionTime = now();
        m_lastResetHearBeatTime = now();
        // 持久化存储中回复raft的状态
        readPersist(m_presister->ReadRaftState());
        //如果m_lastSnapshotIncludeIndex大于0，则将m_lastApplied设置为该值，这是为了确保在崩溃后能够从快照中恢复状态
        if(m_lastSnapshotIncludeIndex > 0){
            m_lastApplied = m_lastSnapshotIncludeIndex;
        }
        DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
            m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);
    }
    // 协程相关设置, 定义在config头文件中
    // const int FIBER_THREAD_NUM = 1;              // 协程库中线程池大小
    // const bool FIBER_USE_CALLER_THREAD = false;  // 是否使用caller_thread执行调度任务
    m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD); 
    
    // start ticker fiber to start elections
    // 启动三个循环定时器
    // todo:原来是启动了三个线程，现在是直接使用了协程，三个函数中leaderHearBeatTicker
    // 、electionTimeOutTicker执行时间是恒定的，applierTicker时间受到数据库响应延迟和两次apply之间请求数量的影响，这个随着数据量增多可能不太合理，最好其还是启用一个线程。
    m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });
    m_ioManager->scheduler([this]() -> void { this->electionTimeOutTicker(); });

    std::thread t3(&Raft::applierTicker, this);
    t3.detach();
    // std::thread t(&Raft::leaderHearBeatTicker, this);
    // t.detach();
    //
    // std::thread t2(&Raft::electionTimeOutTicker, this);
    // t2.detach();
    //
}