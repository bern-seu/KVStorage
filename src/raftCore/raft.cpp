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
    myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
    m_status = Follower; //只要收到合法的AE，说明有leader，要变为follower
    // term相等，重置选举时间
    m_lastResetElectionTime = now();
    // 不能无脑的从prevlogIndex开始阶段日志，因为rpc可能会延迟，导致发过来的log是很久之前的
    //	那么就比较日志，日志有3种情况
    if(args->prevlogindex() > getLastLogIndex()){
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(getLastLogIndex() + 1);
        return;
    }else if(args->prevlogindex() < m_lastSnapshotIncludeIndex){
        // 如果prevlogIndex还没有更上快照
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(m_lastSnapshotIncludeIndex +1);
        // 如果想直接弄到最新好像不对，因为是从后慢慢往前匹配的，这里不匹配说明后面的都不匹配
    }
    // 判断args->prevlogterm()与本地的args->prevlogindex()对应的term是否相同，为什么要判断？
    if(matchLog(args->prevlogindex(), args->prevlogterm()))
    {
        // 遍历 Leader 发来的 entries
        int i = 0;
        for (; i < args->entries_size(); ++i) {
            auto log = args->entries(i);
            
            // 1. 如果 logindex 超出了我现在的范围，说明是全新的日志
            if (log.logindex() > getLastLogIndex()) {
                break; // 跳出循环，直接把剩下的全部 push_back
            }

            // 2. 如果 logindex 在我范围内，比较 Term
            int sliceIdx = getSlicesIndexFromLogIndex(log.logindex());
            if (m_logs[sliceIdx].logterm() == log.logterm()) {
                // 显式校验：Term 相同但 Command 不同是严重的存储层错误
                // (你的 Assert 写得很对)
                continue; // 匹配，继续检查下一条
            } else {
                // 3. !!! 发现冲突 !!!
                // 动作：截断！删除从当前位置开始的所有日志
                // [Cite: Raft Paper Section 5.3] "If an existing entry conflicts with a new one... delete the existing entry and all that follow it."
                long long current_idx = sliceIdx;
                m_logs.erase(m_logs.begin() + current_idx, m_logs.end());
                
                // 既然删除了，状态要更新（比如持久化）
                break; // 跳出匹配循环，准备追加 Leader 的数据
            }
        }

        // 4. 将 entries 中剩余的部分追加到末尾
        for (; i < args->entries_size(); ++i) {
            m_logs.push_back(args->entries(i));
        }
        //前面更新完后，理应getLastLogIndex() >= args->prevlogindex() + args->entries_size()
        myAssert(
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
        if (args->leadercommit() > m_commitIndex) {
            m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
            // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
        }
        //  这里的assert是不是多余了？
        myAssert(getLastLogIndex() >= m_commitIndex,
             format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
        reply->set_success(true);
        reply->set_term(m_currentTerm);
        return;
    }
    else{
        // 优化
        // PrevLogIndex 长度合适，但是不匹配，因此往前寻找 矛盾的term的第一个元素
        // 为什么该term的日志都是矛盾的呢？也不一定都是矛盾的，只是这么优化减少rpc而已
        // ？什么时候term会矛盾呢？很多情况，比如leader接收了日志之后马上就崩溃等等
        reply->set_updatenextindex(args->prevlogindex());
        for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
            if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
                reply->set_updatenextindex(index + 1);
                break;
            }
        }
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        return;
    }
}

//进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex	，而且小于等于rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm){
    myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(), format("不满足:logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
    return logTerm == getLogTermFromLogIndex(logIndex);
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