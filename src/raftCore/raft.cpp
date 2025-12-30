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
            //使用匿名函数执行避免其拿到锁
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
                // 需要选举！
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

// getPrevLogInfo
// leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
    // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
    if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
        //要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
        *preIndex = m_lastSnapshotIncludeIndex;
        *preTerm = m_lastSnapshotIncludeTerm;
        return;
    }
    auto nextIndex = m_nextIndex[server];
    *preIndex = nextIndex - 1;
    *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}


void Raft::GetState(int* term, bool* isLeader) {
    // 构造时自动 lock，析构时（函数结束）自动 unlock
    std::lock_guard<std::mutex> guard(m_mtx);
    *term = m_currentTerm;
    *isLeader = (m_status == Leader);
}

void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) 
{
    std::lock_guard<std::mutex> guard(m_mtx);
    // 1. Term 检查
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);
        return;
    }
    // 2. 发现更大的 Term，更新状态
    if (args->term() > m_currentTerm) {
        m_currentTerm = args->term();
        m_votedFor = -1;
        m_status = Follower;
        persist(); // Term 变了要持久化
    }
    m_status = Follower;
    m_lastResetElectionTime = now(); // 重置选举超时
    // 3. 检查快照是否过时
    // 如果发来的快照还没我现在的快照新，或者是旧日志生成的，直接忽略
    if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
        reply->set_term(m_currentTerm);
        return;
    }
    /// 4. 核心逻辑：决定如何处理本地日志
    // 判断：本地是否存在 index == snapshotIndex 且 term == snapshotTerm 的日志？
    int lastLogIndex = getLastLogIndex();
    bool match = false;
    // 先判断索引是否包含在当前日志范围内
    if (lastLogIndex >= args->lastsnapshotincludeindex()) {
        if (lastLogIndex >= args->lastsnapshotincludeindex() && 
            getLogTermFromLogIndex(args->lastsnapshotincludeindex()) == args->lastsnapshotincludeterm()) {
            match = true;
        }
    }
    if (match) {
        // 情况 A: 本地日志包含快照的最后一条，且 Term 匹配
        // 这意味着快照之后的日志可能还是有效的，我们只截断快照之前的部分
        // 也就是：保留 [args->lastsnapshotincludeindex() + 1, end] 的日志
        
        // 计算要在 vector 中保留的起始位置
        int sliceIdx = getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex());
        
        // erase 删除 [first, last)，我们要删除到 sliceIdx 那个位置（包含它，因为它进快照了）
        // 所以保留从 sliceIdx + 1 开始的内容
        std::vector<raftRpcProctoc::LogEntry> newLogs;
        if (sliceIdx + 1 < m_logs.size()) {
             newLogs.assign(m_logs.begin() + sliceIdx + 1, m_logs.end());
        }
        m_logs = newLogs; 
    } else {
        // 情况 B: 日志冲突或日志过时太多了
        // 直接清空所有日志，因为快照代表了绝对真理
        m_logs.clear();
    }
    // 5. 更新状态
    m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
    m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();
    m_commitIndex = std::max(m_commitIndex, m_lastSnapshotIncludeIndex);
    m_lastApplied = std::max(m_lastApplied, m_lastSnapshotIncludeIndex);
    m_persister->Save(persistData(), args->data());
    reply->set_term(m_currentTerm);
    // 7. 构建消息
    ApplyMsg msg;
    msg.SnapshotValid = true;
    msg.Snapshot = args->data();
    msg.SnapshotTerm = args->lastsnapshotincludeterm();
    msg.SnapshotIndex = args->lastsnapshotincludeindex();
    // 手动解锁，避免推送到满队列时持有 Raft 锁导致整个 Raft 卡死
    m_mtx.unlock(); 
    applyChan->Push(msg);
    // auto lastLogIndex = getLastLogIndex();
    // if (lastLogIndex > args->lastsnapshotincludeindex()) {
    //     m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
    // } else {
    //     m_logs.clear();
    // }
    // m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
    // m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
    // m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
    // m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();
    // reply->set_term(m_currentTerm);
    // ApplyMsg msg;
    // msg.SnapshotValid = true;
    // msg.Snapshot = args->data();
    // msg.SnapshotTerm = args->lastsnapshotincludeterm();
    // msg.SnapshotIndex = args->lastsnapshotincludeindex();
    // std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 创建新线程并执行b函数，并传递参数
    // t.detach();
    // //看下这里能不能再优化
    // //    DPrintf("[func-InstallSnapshot-rf{%v}] receive snapshot from {%v} ,LastSnapShotIncludeIndex ={%v} ", rf.me,
    // //    args.LeaderId, args.LastSnapShotIncludeIndex)
    // //持久化
    // m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

void Raft::leaderHearBeatTicker() {
    // 使用 static atomic 保证计数器在函数调用间持久且线程安全
    static std::atomic<int32_t> heartBeatCount = 0;

    while (true) {
        // 1. 非 Leader 时的处理：
        // 只要不是 Leader，就挂起，稍微睡一下避免死循环空转 CPU
        if (m_status != Leader) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }

        // 2. 计时开始
        auto start = std::chrono::steady_clock::now();

        // 3. 执行心跳 (核心逻辑)
        doHeartBeat();

        // 4. 计时结束
        auto end = std::chrono::steady_clock::now();
        std::chrono::duration<double, std::milli> duration = end - start;

        // 5. 终端输出
        {
            std::cout << "\033[1;35m" // 紫色开始
                      << "[Leader Heartbeat] "
                      << "Node:" << m_me << " "
                      << "Count:" << heartBeatCount++ << " "
                      << "Cost:" << duration.count() << "ms"
                      << "\033[0m" // 颜色重置
                      << std::endl;
        }

        // 6. 周期性睡眠
        // Leader 发完一轮心跳后，休息一个 HeartBeatTimeout 再发下一轮
        std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
    }
}
void Raft::persist() {
  auto data = persistData();
  m_persister->SaveRaftState(data);
  // fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm,
  // rf.votedFor, rf.logs) fmt.Printf("%v\n", string(data))
}

void Raft::leaderSendSnapShot(int server) {
    m_mtx.lock();
    raftRpcProctoc::InstallSnapshotRequest args;
    args.set_leaderid(m_me);
    args.set_term(m_currentTerm);
    args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
    args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
    args.set_data(m_persister->ReadSnapshot());
    raftRpcProctoc::InstallSnapshotResponse reply;
    m_mtx.unlock();
    bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
    std::lock_guard<std::mutex> locker(m_mtx);
    if (!ok) {
        return;
    }
    if (m_status != Leader || m_currentTerm != args.term()) {
        return;  //中间释放过锁，可能状态已经改变了
    }
    //	无论什么时候都要判断term
    if (reply.term() > m_currentTerm) {
        //三变
        m_currentTerm = reply.term();
        m_votedFor = -1;
        m_status = Follower;
        persist();
        m_lastResetElectionTime = now();
        return;
    }
    //这里是lastsnapshotincludeindex，是因为安装之后，它就是最新的
    m_matchIndex[server] = std::max(m_matchIndex[server], args.lastsnapshotincludeindex());
    m_nextIndex[server] = m_matchIndex[server] + 1;
}

void Raft::leaderUpdateCommitIndex() {
    m_commitIndex = std::max(m_lastSnapshotIncludeIndex,m_commitIndex);
    for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--) {
        int sum = 0;
        for (int i = 0; i < m_peers.size(); i++) {
            if (i == m_me) {
                sum += 1;
                continue;
            }
            if (m_matchIndex[i] >= index) {
                sum += 1;
            }
        }
        if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
            m_commitIndex = index;
            break;
        }
    }
}

void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
    std::lock_guard<std::mutex> lg(m_mtx);
    //对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
    // reason: 出现网络分区，该竞选者已经OutOfDate(过时）
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);
        reply->set_votestate(Expire);
        reply->set_votegranted(false);
        return;
    }
    if (args->term() > m_currentTerm) {
        //        DPrintf("[	    func-RequestVote-rf(%v)		] : 变成follower且更新term
        //        因为candidate{%v}的term{%v}> rf{%v}.term{%v}\n ", rf.me, args.CandidateId, args.Term, rf.me,
        //        rf.currentTerm)
        m_status = Follower;
        m_currentTerm = args->term();
        m_votedFor = -1;
        persist(); // 【关键】状态变了，必须持久化
        //	重置定时器：收到leader的ae，开始选举，透出票
        //这时候更新了term之后，votedFor也要置为-1
    }
    myAssert(args->term() == m_currentTerm,
           format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));
    //	现在节点任期都是相同的(任期小的也已经更新到新的args的term了)，还需要检查log的term和index是不是匹配的了
    int lastLogTerm = getLastLogTerm();
    //只有没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会授票
    if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
        // args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
        //日志太旧了
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);

        return;
    }
    // todo ： 啥时候会出现rf.votedFor == args.CandidateId ，就算candidate选举超时再选举，其term也是不一样的呀
    //     当因为网络质量不好导致的请求丢失重发就有可能！！！！
    if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
        //        DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because has voted\n",
        //        rf.me, args.CandidateId)
        reply->set_term(m_currentTerm);
        reply->set_votestate(Voted);
        reply->set_votegranted(false);

        return;
    }
    if (m_votedFor != args->candidateid()) {
        m_votedFor = args->candidateid();
        //m_status = Follower; // 确保状态正确，虽然前面可能设置过了
        persist(); 
    }

    // 无论是第一次投票，还是重试请求，都要重置定时器并返回 True
    m_lastResetElectionTime = now(); 
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);
    return;
}

bool Raft::UpToDate(int index, int term) {
  // lastEntry := rf.log[len(rf.log)-1]
    int lastIndex = -1;
    int lastTerm = -1;
    getLastLogIndexAndTerm(&lastIndex, &lastTerm);
    return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

int Raft::getLogTermFromLogIndex(int logIndex) {
    myAssert(logIndex >= m_lastSnapshotIncludeIndex,
            format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                    logIndex, m_lastSnapshotIncludeIndex));

    int lastLogIndex = getLastLogIndex();

    myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                                m_me, logIndex, lastLogIndex));

    if (logIndex == m_lastSnapshotIncludeIndex) {
        return m_lastSnapshotIncludeTerm;
    } else {
        return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
    }
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// 找到index对应的真实下标位置！！！
// 限制，输入的logIndex必须保存在当前的logs里面（不包含snapshot）
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
    myAssert(logIndex > m_lastSnapshotIncludeIndex,
            format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                    logIndex, m_lastSnapshotIncludeIndex));
    int lastLogIndex = getLastLogIndex();
    myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                                m_me, logIndex, lastLogIndex));
    int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
    return SliceIndex;
}

bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<std::atomic<int>> votedNum) 
{
    
    auto start = now();
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 開始", m_me, m_currentTerm, getLastLogIndex());
    //ok 的含义：它代表 RPC 通信层 是否成功。如果通信失败，这次交互就应该被当做“没发生过”。
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 完畢，耗時:{%d} ms", m_me, m_currentTerm,
            getLastLogIndex(), now() - start);
    if (!ok) {
        return ok;  
    }
    //对回应进行处理，要记得无论什么时候收到回复就要检查term
    std::lock_guard<std::mutex> lg(m_mtx);
    // 如果在等待 RPC 期间，我已经不是 Candidate 了（比如收到了 Leader 心跳变成了 Follower）
    // 或者我的 Term 已经变了（比如超时开始了新一轮选举）
    // 那么这次 RPC 的结果已经失效，必须丢弃。
    if (m_status != Candidate || m_currentTerm != args->term()) {
        return true; 
    }
    // 3. 处理 Term 更新
    if (reply->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        persist();
        return true;
    }
    // 4. 处理投票结果
    if (reply->votegranted()){
        *votedNum = *votedNum + 1;
        if (*votedNum >= m_peers.size() / 2 + 1) {
            // 如果已经是 Leader 了，说明之前的票已经够了，这张是多余的票
            // 直接返回
            if (m_status == Leader) {
                return true; 
            }

            // 第一次达到多数派，上位！
            m_status = Leader;
            DPrintf("[func-sendRequestVote rf{%d}] elect success, term:{%d}\n", m_me, m_currentTerm);

            int lastLogIndex = getLastLogIndex();
            // 初始化 Leader 状态
            for (int i = 0; i < m_peers.size(); i++) {
                // nextIndex 初始化为 Leader 最后一条日志 + 1
                m_nextIndex[i] = lastLogIndex + 1; 
                m_matchIndex[i] = 0; 
            }

            // 5. 【立即广播心跳】
            // 必须 detach 防止死锁（doHeartBeat 内部也会拿锁）
            std::thread t(&Raft::doHeartBeat, this); 
            t.detach();
            persist();
        }
    }
    return true;
}

bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<std::atomic<int>> appendNums) 
{
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc開始 ， args->entries_size():{%d}", m_me,
            server, args->entries_size());
            
    // 1. 发送 RPC
    bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());
    if (!ok) {
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失敗", m_me, server);
        return ok;
    }

    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
    
    // 2. 检查连接状态 (ButtonRPC 特有)
    if (reply->appstate() == Disconnected) {
        return ok;
    }

    std::lock_guard<std::mutex> lg1(m_mtx);

    // 3. 处理 Term 更大的情况 (发现新 Leader)
    if (reply->term() > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = reply->term();
        m_votedFor = -1;
        persist(); // 【关键修改】状态变更必须持久化！
        return ok;
    } 
    // 处理 Term 更小的情况 (过期的回复)
    else if (reply->term() < m_currentTerm) {
        DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
                m_me, m_currentTerm);
        return ok;
    }

    // Double Check: 如果在等待 RPC 期间身份变了，不再处理
    if (m_status != Leader) {
        return ok;
    }

    // Term 必须相等 (代码健壮性检查)
    myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));

    // 4. 处理日志不一致 (Success == False)
    if (!reply->success()) {
        // 利用 Follower 的 Hint 快速回溯 NextIndex
        if (reply->updatenextindex() != -100) {
            DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
              server, reply->updatenextindex());
            m_nextIndex[server] = reply->updatenextindex(); 
        }
        // 如果这里没有 else 逻辑，可能需要一个兜底回退，比如 m_nextIndex[server]--
        // 但如果 AppendEntries 实现保证了 updatenextindex 有效，则不需要
    } 
    // 5. 处理成功 (Success == True)
    else {      
        // 更新 matchIndex (使用 max 防止乱序包导致回退)
        m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
        
        // 更新 nextIndex
        m_nextIndex[server] = m_matchIndex[server] + 1;

        int lastLogIndex = getLastLogIndex();
        myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                    m_logs.size(), server, lastLogIndex));

        // 只要 matchIndex 更新了，就尝试计算一下看看能不能推动 commitIndex
        // 这个函数内部包含了 "多数派检查" 和 "当前Term检查"
        leaderUpdateCommitIndex(); 
    }

    return ok;
}

void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
    AppendEntries(request, response); //调用本地方法
    done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
    InstallSnapshot(request, response);

    done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
    RequestVote(request, response);
    done->Run();
}
//整个 Raft 系统启动共识流程的唯一入口,是 KVServer 叫 Raft “干活” 的入口（提交新任务）
//todo:修改架构，不能在底层调用asstring序列化，序列化应该是KVserver完成，交给底层的应该是string数据
void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
    std::lock_guard<std::mutex> lg1(m_mtx);
    if (m_status != Leader) { //但是初始化是每个节点都是Follower，这样不是没法start吗？
        DPrintf("[func-Start-rf{%d}]  is not leader");
        *newLogIndex = -1;
        *newLogTerm = -1;
        *isLeader = false;
        return;
    }
    raftRpcProctoc::LogEntry newLogEntry;
    newLogEntry.set_command(command.asString());
    newLogEntry.set_logterm(m_currentTerm);
    newLogEntry.set_logindex(getNewCommandIndex());
    m_logs.emplace_back(newLogEntry);
    int lastLogIndex = getLastLogIndex();
    DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
    persist();
    *newLogIndex = newLogEntry.logindex();
    *newLogTerm = newLogEntry.logterm();
    *isLeader = true;
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

std::string Raft::persistData() {
    BoostPersistRaftNode boostPersistRaftNode;
    boostPersistRaftNode.m_currentTerm = m_currentTerm;
    boostPersistRaftNode.m_votedFor = m_votedFor;
    boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
    boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
    for (auto& item : m_logs) {
        boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << boostPersistRaftNode;
    return ss.str();
}
void Raft::readPersist(std::string data) {//这里似乎是直接从内存读
    if (data.empty()) {
        return;
    }
    std::stringstream iss(data);
    boost::archive::text_iarchive ia(iss);
    // read class state from archive
    BoostPersistRaftNode boostPersistRaftNode;
    ia >> boostPersistRaftNode;

    m_currentTerm = boostPersistRaftNode.m_currentTerm;
    m_votedFor = boostPersistRaftNode.m_votedFor;
    m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
    m_logs.clear();
    for (auto& item : boostPersistRaftNode.m_logs) {
        raftRpcProctoc::LogEntry logEntry;
        logEntry.ParseFromString(item);
        m_logs.emplace_back(logEntry);
    }
}

void Raft::Snapshot(int index, std::string snapshot) {
    std::lock_guard<std::mutex> lg(m_mtx);

    // 1. 检查快照是否过时，或者是否试图快照未提交的日志
    if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
        DPrintf("[Snapshot-rf{%d}] reject snapshot index %d (currentSnapshotIndex %d, commitIndex %d)",
                m_me, index, m_lastSnapshotIncludeIndex, m_commitIndex);
        return;
    }

    // 记录旧状态用于 Debug/Assert
    int lastLogIndex = getLastLogIndex(); 

    // 2. 保存快照元数据
    // 一定要先取 Term，再截断日志，否则截断后 index 可能就取不到了
    int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
    
    // 3. 截断日志 (Log Truncation)
    // 我们需要保留 index 之后的所有日志 (index+1, index+2 ...)
    int dropEndSlicesIdx = getSlicesIndexFromLogIndex(index);
    
    // 如果 m_logs 里还有比 index 更新的日志，保留它们
    if (dropEndSlicesIdx + 1 < m_logs.size()) {
        std::vector<raftRpcProctoc::LogEntry> newLogs(
            std::make_move_iterator(m_logs.begin() + dropEndSlicesIdx + 1),
            std::make_move_iterator(m_logs.end())
        );
        m_logs = std::move(newLogs);
    } else {
        m_logs.clear();
    }

    // 4. 更新快照元数据
    m_lastSnapshotIncludeIndex = index;
    m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;

    // 5. 更新状态机位置
    // 因为既然生成了 index 的快照，说明 index 及其之前的数据已经被状态机“消化”了
    m_lastApplied = std::max(m_lastApplied, index);
    
    // m_commitIndex 不需要更新，因为前提就是 index <= m_commitIndex

    // 6. 持久化 (保存 Raft 状态 + 应用层快照)
    m_persister->Save(persistData(), snapshot);

    DPrintf("[Snapshot] Server %d snapshot success. index:%d, term:%d, log_remain:%d", 
            m_me, index, m_lastSnapshotIncludeTerm, m_logs.size());
            
    // 校验：现在的快照Index + 剩余日志长度 应该等于 之前的总日志长度
    // 注意：只有当没有任何新日志在 snapshot 期间被追加进来时，这个等式才严格成立。
    // 在持锁情况下是成立的。
    myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
             format("Log len mismatch: %d + %d != %d", m_logs.size(), m_lastSnapshotIncludeIndex, lastLogIndex));
}