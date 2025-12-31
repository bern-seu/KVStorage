#include "kvServer.h"
#include "rpcprovider.h"
#include "mprpcconfig.h"

void KvServer::DprintfKVDB() {
    if (!Debug) {
        return;
    }
    //std::lock_guard<std::mutex> lg(m_mtx);
    m_skipList.display_list();
}

void KvServer::ExecuteAppendOpOnKVDB(Op op) {
    std::lock_guard<std::mutex> locker(m_mtx);
    // 1. 先尝试获取旧值
    std::string value;
    if (m_skipList.search_element(op.Key, value)) {
        // 2. 如果存在，追加
        value += op.Value;
        m_skipList.insert_set_element(op.Key, value);
    } else {
        // 3. 如果不存在，直接作为新值
        m_skipList.insert_set_element(op.Key, op.Value);
    }
    
    // 更新去重表
    m_lastRequestId[op.ClientId] = op.RequestId;
    //DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
    std::lock_guard<std::mutex> locker(m_mtx);
    *value = "";
    *exist = false;
    if (m_skipList.search_element(op.Key, *value)) {
        *exist = true;
    }
    m_lastRequestId[op.ClientId] = op.RequestId;
    //DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
    std::lock_guard<std::mutex> locker(m_mtx);
    m_skipList.insert_set_element(op.Key, op.Value);
    m_lastRequestId[op.ClientId] = op.RequestId;
    //DprintfKVDB();
}

// 处理来自clerk的Get RPC
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
    Op op;
    op.Operation = "Get";
    op.Key = args->key();
    op.Value = "";
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int raftIndex = -1; 
    int _ = -1;
    bool isLeader = false;
    m_raftNode->Start(op, &raftIndex, &_,
                    &isLeader);
    if (!isLeader) {
        reply->set_err(ErrWrongLeader);
        return;
    }
    std::unique_lock<std::mutex> locker(m_mtx);
    //raftIndex其实是新命令应该分配的logindex，这里应该是一个命令对应一个Op，为什么还要LockQueue<Op>？
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    LockQueue<Op>* chForRaftIndex = waitApplyCh[raftIndex];
    locker.unlock(); //直接解锁，等待任务执行完成，不能一直拿锁等待

    // timeout
    Op raftCommitOp;
    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
        int _ = -1;
        bool isLeader = false;
        m_raftNode->GetState(&_, &isLeader);
        if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
            // 如果超时，代表raft集群不保证已经commitIndex该日志，但是如果是已经提交过的get请求，是可以再执行的。
            // 但这样破坏了强线性一致性，其实这里的框架有问题，不应该到了这一步才判断
            reply->set_err(ErrWrongLeader);  //返回这个，其实就是让clerk换一个节点重试
        } 
    } else {
        // todo 这里还要再次检验的原因：感觉不用检验，因为leader只要正确的提交了，那么这些肯定是符合的
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist) {
                reply->set_err(OK);
                reply->set_value(value);
            } else {
                reply->set_err(ErrNoKey);
                reply->set_value("");
            }
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }
    locker.lock();
    //这里为什么还要再来一个tmp，前面不是有一个chForRaftIndex了吗？
    auto tmp = waitApplyCh[raftIndex];//这里
    waitApplyCh.erase(raftIndex);
    delete tmp;
} 

void KvServer::GetCommandFromRaft(ApplyMsg message) {
    Op op;
    op.parseFromString(message.Command);

}

