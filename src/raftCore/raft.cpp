#include "raft.h"
#include "config.h"
#include <chrono>

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
        m_status = Follower;
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
                if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
                        m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
                    //相同位置的log ，其logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了！
                    myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                            " {%d:%d}却不同！！\n",
                                            m_me, log.logindex(), log.logterm(), m_me,
                                            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                            log.command()));
                }
                continue; // 匹配，继续检查下一条
            } else {
                // 3. !!! 发现冲突 !!!
                // 动作：截断！删除从当前位置开始的所有日志
                // [Cite: Raft Paper Section 5.3] "If an existing entry conflicts with a new one... delete the existing entry and all that follow it."
                int current_idx = sliceIdx;
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

void Raft::applierTicker(){
    while(true){
        m_mtx.lock();
        if(m_status == Leader){
            DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
            m_commitIndex);
        }
        auto applyMsgs = getApplyLogs();
        m_mtx.unlock();
        // todo:好像必须拿锁，因为不拿锁的话如果调用多次applyLog函数，可能会导致应用的顺序不一样
        if (!applyMsgs.empty()) {
            DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver報告的applyMsgs長度爲：{%d}", m_me, applyMsgs.size());
        }
        for (auto& message : applyMsgs) {
            applyChan->Push(message);
        }
        sleepNMilliseconds(ApplyInterval);
    }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
    std::lock_guard<std::mutex> locker(m_mtx);
    // 1. 检查快照是否过时
    // 如果在处理快照的过程中，我们已经提交了比这个快照更新的日志 (commitIndex >= lastIncludedIndex)，
    // 说明这个快照已经旧了，不能安装，否则会把数据回滚到旧状态
    if (lastIncludedIndex <= m_commitIndex) {
        DPrintf("[CondInstallSnapshot] Refused. Snapshot index{%d} <= commitIndex{%d}", lastIncludedIndex, m_commitIndex);
        return false;
    }
    // 2. 日志裁剪 (Log Truncation)
    // 我们需要保留那些“比快照还新”的日志，丢弃那些“已经被快照包含”的日志。
    int lastLogIndex = getLastLogIndex();

    if (lastIncludedIndex > lastLogIndex){
        // 直接清空所有日志，因为它们都被快照覆盖了
        m_logs.clear();
    }else{
        // 保留 lastIncludedIndex 之后的部分
        // 获取 lastIncludedIndex 在当前 m_logs 数组中的下标
        int sliceIdx = getSlicesIndexFromLogIndex(lastIncludedIndex);
        if (sliceIdx != -1) {
             // 把 index <= lastIncludedIndex 的所有日志删掉
             m_logs.erase(m_logs.begin(), m_logs.begin() + sliceIdx + 1);
        } else {
             // 理论上不应该发生，如果找不到 index，为了安全起见清空
             m_logs.clear();
        }
    }
    // 3. 更新快照元数据
    // 更新记录的“快照末尾”信息
    m_lastSnapshotIncludeIndex = lastIncludedIndex;
    m_lastSnapshotIncludeTerm = lastIncludedTerm;
    // 4. 更新核心状态
    // 快照代表这些数据已经被提交并应用了，所以直接推进 commitIndex 和 lastApplied
    m_commitIndex = lastIncludedIndex;
    m_lastApplied = lastIncludedIndex;
    // 5. 持久化
    // persistData() 会根据上面更新后的 m_logs 和 m_lastSnapshotIncludeIndex 生成新的状态数据
    m_persister->Save(persistData(), snapshot);
    return true;
}

//进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex	，而且小于等于rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm){
    myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(), format("不满足:logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
    return logTerm == getLogTermFromLogIndex(logIndex);
}

void Raft::doElection() {
    std::lock_guard<std::mutex> g(m_mtx);
    if (m_status != Leader){
        DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);
        //当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡主
        //重竞选超时，term也会增加的
        m_status = Candidate;
        ///开始新一轮的选举
        m_currentTerm += 1;
        m_votedFor = m_me;  //即是自己给自己投，也避免candidate给同辈的candidate投
        persist();//为什么这里需要持久化？因为当currentTerm、votedFor、log[]这三个状态必须在修改后立即持久化到磁盘
        auto votedNum = std::make_shared<std::atomic<int>>(1);
        //	重新设置定时器
        m_lastResetElectionTime = now();
        //	发布RequestVote RPC
        for(int i = 0; i < m_peers.size();++i){
            if (i == m_me) {
                continue;
            }
            int lastLogIndex = -1, lastLogTerm = -1;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  //获取最后一个log的term和下标
            std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
                std::make_shared<raftRpcProctoc::RequestVoteArgs>();
            requestVoteArgs->set_term(m_currentTerm);
            requestVoteArgs->set_candidateid(m_me);
            requestVoteArgs->set_lastlogindex(lastLogIndex);
            requestVoteArgs->set_lastlogterm(lastLogTerm);
            auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();
            //使用匿名函数执行避免其拿到锁,为什么这里传入了this指针，创建一个线程必须要传入this指针吗？直接传入函数地址和参数不行吗？
            std::thread t(&Raft::sendRequestVote,this,i,requestVoteArgs,requestVoteReply,votedNum);
            t.detach();
        }
    }
}

void Raft::doHeartBeat() {
    std::lock_guard<std::mutex> g(m_mtx);
    if(m_status == Leader){
        DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
        //使用 atomic 防止并发写冲突
        auto appendNums = std::make_shared<std::atomic<int>>(1);  //正确返回的节点的数量
        //对Follower（除了自己外的所有节点发送AE）
        // todo 这里肯定是要修改的，最好使用一个单独的goruntime来负责管理发送log，因为后面的log发送涉及优化之类的
        // 最少要单独写一个函数来管理，而不是在这一坨
        for(int i = 0;i<m_peers.size();++i){
            if (i == m_me) {
                continue;
            }
            DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
            myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
            //日志压缩加入后要判断是发送快照还是发送AE
            if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
                std::thread t(&Raft::leaderSendSnapShot, this, i);  // 创建新线程并执行b函数，并传递参数
                t.detach();
                continue;
            }
            //构造发送值
            int preLogIndex = -1;
            int PrevLogTerm = -1;
            getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
            std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
                std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
            appendEntriesArgs->set_term(m_currentTerm);
            appendEntriesArgs->set_leaderid(m_me);
            appendEntriesArgs->set_prevlogindex(preLogIndex);
            appendEntriesArgs->set_prevlogterm(PrevLogTerm);
            appendEntriesArgs->clear_entries();
            appendEntriesArgs->set_leadercommit(m_commitIndex);
            // 如果 preLogIndex 是快照结尾，startIdx 应该是 0
            // 如果 preLogIndex 是日志中间，startIdx 应该是那个位置的下一个
            int startIdx = getSlicesIndexFromLogIndex(preLogIndex) + 1;
            for (int j = startIdx; j < m_logs.size(); ++j) {
                raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
                *sendEntryPtr = m_logs[j]; // Protobuf 自动深拷贝
            }
            int lastLogIndex = getLastLogIndex();
            // leader对每个节点发送的日志长短不一，但是都保证从preLogIndex发送直到最后
            myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
            //  构造返回值,const修饰指针，是常量指针，可以改变值，不能改变指向
            const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
                    std::make_shared<raftRpcProctoc::AppendEntriesReply>();
            appendEntriesReply->set_appstate(Disconnected);
            std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply,
                    appendNums);  // 创建新线程并执行b函数，并传递参数
            t.detach();
        }
        m_lastResetHearBeatTime = now();  // leader发送心跳，就不是随机时间了
    }
}

void Raft::electionTimeOutTicker() {
    while (true) {
        // 1. 如果是 Leader，就休息，等待身份变化
        // 注意：这里需要加锁读取 m_status，或者 m_status 是原子变量
        while (m_status == Leader) { 
            std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
        }

        // 2. 生成一个随机超时时间 (比如 300ms ~ 500ms)
        std::chrono::milliseconds timeout = getRandomizedElectionTimeout();
        
        // 3. 记录睡眠开始时的“上次心跳时间”
        std::chrono::system_clock::time_point lastLogTime;
        {
            std::lock_guard<std::mutex> locker(m_mtx);
            lastLogTime = m_lastResetElectionTime;
        }

        // 4. 睡眠这段时间
        std::this_thread::sleep_for(timeout);

        // 5. 睡醒了！检查睡眠期间有没有收到过新消息
        bool needToElect = false; // 定义一个标志位
        {
            std::lock_guard<std::mutex> locker(m_mtx);
            
            // 如果睡眠期间 m_lastResetElectionTime 变大了，说明收到了心跳
            // 检查逻辑：如果收到了心跳，或者已经不是 Follower 了
            if (m_lastResetElectionTime > lastLogTime || m_status == Leader) {
                // 不需要选举，do nothing
            } else {
                // 需要选举！但不要在这里调用 doElection，只设置标志位
                needToElect = true; 
            }
        }
        if (needToElect) {
            // doElection 内部会自己抢锁，不会死锁
            // doElection 内部拿到锁后，建议再做一次 Double Check (再次检查状态)，
            // 因为在“解锁”到“进入 doElection”的微小时间窗口内，状态可能变了。
            doElection(); 
        }
    }
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
    //std::lock_guard<std::mutex> guard(m_mtx);
    std::vector<ApplyMsg> applyMsgs;
    myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));
    while(m_lastApplied < m_commitIndex){
        m_lastApplied++;
         myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
        ApplyMsg applyMsg;
        applyMsg.CommandValid = true;
        applyMsg.SnapshotValid = false;
        applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
        applyMsg.CommandIndex = m_lastApplied;
        applyMsgs.emplace_back(applyMsg);
    }
    return applyMsgs;
}
// 获取新命令应该分配的Index
int Raft::getNewCommandIndex() {
  //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
  if(m_logs.empty()) return m_lastSnapshotIncludeIndex + 1;
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}
int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}
int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}
void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  } else {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}


void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh)
{
    //与其他节点沟通的rpc类
    m_peers = peers;
    //持久化类
    m_persister = persister;
    //标记自己，不能给自己发送rpc
    m_me = me;
    m_mtx.lock();
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
    readPersist(m_persister->ReadRaftState());
    //如果m_lastSnapshotIncludeIndex大于0，则将m_lastApplied设置为该值，这是为了确保在崩溃后能够从快照中恢复状态
    if(m_lastSnapshotIncludeIndex > 0){
        m_lastApplied = m_lastSnapshotIncludeIndex;
    }
    DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
        m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);
    m_mtx.unlock();
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