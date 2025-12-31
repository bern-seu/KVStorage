#ifndef SKIP_LIST_ON_RAFT_KVSERVER_H
#define SKIP_LIST_ON_RAFT_KVSERVER_H
//注意看看如何优化
#include "skipList.h"
#include "kvServerRPC.pb.h"
#include "raft.h"

class KvServer : raftKVRpcProctoc::kvServerRpc {
private:
    std::mutex m_mtx;
    int m_me;
    std::shared_ptr<Raft> m_raftNode;
    std::shared_ptr<LockQueue<ApplyMsg> > applyChan; // kvServer和raft节点的通信管道
    int m_maxRaftState;     // snapshot if log grows this big

    std::string m_serializedKVData; // todo ： 序列化后的kv数据，理论上可以不用，但是目前没有找到特别好的替代方法
    SkipList<std::string, std::string> m_skipList; //跳表（Skip List）是一种概率型数据结构，基于多层链表实现。它的查找、插入、删除操作的平均时间复杂度都是 $O(\log N)$。
    std::unordered_map<std::string, std::string> m_kvDB;

    std::unordered_map<int, LockQueue<Op>*> waitApplyCh; ////这个Op是kvserver传递给raft的command,后续考虑重构成string

    std::unordered_map<std::string, int> m_lastRequestId;

    int m_lastSnapShotRaftLogIndex;

public:
    KvServer() = delete;

    KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

    void StartKVServer();

    void DprintfKVDB();

    void ExecuteAppendOpOnKVDB(Op op);

    void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

    void ExecutePutOpOnKVDB(Op op);

    void Get(const raftKVRpcProctoc::GetArgs *args,
           raftKVRpcProctoc::GetReply
               *reply);  //将 GetArgs 改为rpc调用的，因为是远程客户端，即服务器宕机对客户端来说是无感的
    
    void GetCommandFromRaft(ApplyMsg message);

    //去重，检查 m_lastRequestId 表。如果 RequestId <= lastRequestId，说明是重复请求，直接丢弃或返回旧结果。
    bool ifRequestDuplicate(std::string ClientId, int RequestId);

    // clerk 使用RPC远程调用
    void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

    //一直等待raft传来的applyCh
    void ReadRaftApplyCommandLoop();

    void ReadSnapShotToInstall(std::string snapshot);

    bool SendMessageToWaitChan(const Op &op, int raftIndex);

    // 检查是否需要制作快照，需要的话就向raft之下制作快照
    void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

    // Handler the SnapShot from kv.rf.applyCh
    void GetSnapShotFromRaft(ApplyMsg message);

    std::string MakeSnapShot();

public:// for rpc
    void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;

    void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) override;
private:
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)  //这里面写需要序列话和反序列化的字段
    {
        ar &m_serializedKVData;

        // ar & m_kvDB;
        ar &m_lastRequestId;
    }
    std::string getSnapshotData() {
        m_serializedKVData = m_skipList.dump_file();
        std::stringstream ss;
        boost::archive::text_oarchive oa(ss);
        oa << *this;
        m_serializedKVData.clear();
        return ss.str();
    }
    void parseFromString(const std::string &str) {
        std::stringstream ss(str);
        boost::archive::text_iarchive ia(ss);
        ia >> *this;
        m_skipList.load_file(m_serializedKVData);
        m_serializedKVData.clear();
    }
};

#endif  // SKIP_LIST_ON_RAFT_KVSERVER_H