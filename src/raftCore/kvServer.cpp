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
    //m_lastRequestId[op.ClientId] = op.RequestId;
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
    DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
        return;
    }
    if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
        // execute command
        if (op.Operation == "Put") {
            ExecutePutOpOnKVDB(op);
        }
        if (op.Operation == "Append") {
            ExecuteAppendOpOnKVDB(op);
        }
        //  kv.lastRequestId[op.ClientId] = op.RequestId  在Executexxx函数里面更新的
    }
    //到这里kvDB已经制作了快照
    if (m_maxRaftState != -1) {
        IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
        //如果raft的log太大（大于指定的比例）就把制作快照
    }

    // Send message to the chan of op.ClientId
    SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
        return false;
        // todo :不存在这个client就创建
    }
    return RequestId <= m_lastRequestId[ClientId];
}

// get和put//append執行的具體細節是不一樣的
// PutAppend在收到raft消息之後執行，具體函數裏面只判斷冪等性（是否重複）
// get函數收到raft消息之後在，因爲get無論是否重複都可以再執行
void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
    Op op;
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = args->value();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int raftIndex = -1;
    int _ = -1;
    bool isleader = false;

    // 1. 发起 Raft 共识
    m_raftNode->Start(op, &raftIndex, &_, &isleader);

    if (!isleader) {
        reply->set_err(ErrWrongLeader);
        return;
    }

    // 2. 注册等待通道
    std::unique_lock<std::mutex> locker(m_mtx);
    
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
    }
    auto chForRaftIndex = waitApplyCh[raftIndex];
    locker.unlock();

    // 3. 等待结果
    Op raftCommitOp;
    if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
        // 超时直接报错！
        // 哪怕是重复请求，超时也意味着当前节点可能处于网络分区中
        // 应该让 Client 去重试连接新 Leader
        DPrintf("[Timeout] Server %d Index %d RequestId %d", m_me, raftIndex, op.RequestId);
        reply->set_err(ErrWrongLeader); 
    } else {
        // 4. 收到结果，验证是否是当前请求
        // 检查 Op 里的 ClientId 和 RequestId 是否匹配/
        if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
            reply->set_err(OK);
        } else {
            // Index 被其他 Term 的日志覆盖了
            reply->set_err(ErrWrongLeader);
        }
    }

    // 5. 清理资源
    locker.lock();
    if (waitApplyCh.find(raftIndex) != waitApplyCh.end()) {
        auto tmp = waitApplyCh[raftIndex];
        waitApplyCh.erase(raftIndex);
        delete tmp;
    }
    locker.unlock();
}

//
void KvServer::ReadRaftApplyCommandLoop() {
    while (true) {
        //如果只操作applyChan不用拿锁，因为applyChan自己带锁
        auto message = applyChan->Pop();  //阻塞弹出
        DPrintf(
            "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
            m_me);
        // listen to every command applied by its raft ,delivery to relative RPC Handler

        if (message.CommandValid) {
            GetCommandFromRaft(message);
        }
        if (message.SnapshotValid) {
            GetSnapShotFromRaft(message);
        }
    }
}

//  raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
    if (snapshot.empty()) {
        return;
    }
    parseFromString(snapshot);
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
    std::lock_guard<std::mutex> lg(m_mtx);
    DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
        return false;
    }
    waitApplyCh[raftIndex]->Push(op);
     DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    return true;
}

//存档用
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
    if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10) {
        // Send SnapShot Command
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftIndex, snapshot);
    }
}

//读档用
void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
    std::lock_guard<std::mutex> lg(m_mtx);

    if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
        ReadSnapShotToInstall(message.Snapshot);
        m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
    }
}

std::string KvServer::MakeSnapShot() {
    std::lock_guard<std::mutex> lg(m_mtx);
    std::string snapshotData = getSnapshotData();
    return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {
    KvServer::PutAppend(request, response);
    done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
    KvServer::Get(request, response);
    done->Run();
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) : m_skipList(6) {
    std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);
    m_me = me;
    m_maxRaftState = maxraftstate;
    applyChan = std::make_shared<LockQueue<ApplyMsg>> ();
    m_raftNode = std::make_shared<Raft>();
    ////////////////clerk层面 kvserver开启rpc接受功能
    //    同时raft与raft节点之间也要开启rpc功能，因此有两个注册
    std::thread t([this,port]() -> void {
        // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
        RpcProvider provider;
        provider.NotifyService(this);//为什么这里可以直接传this指针，它要的是google::protobuf::Service变量啊，this是raftKVRpcProctoc::kvServerRpc
        provider.NotifyService(
            this->m_raftNode.get());
        // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
        provider.Run(m_me, port);
    });
    t.detach();
    ////开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
    ////这里使用睡眠来保证
    std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
    sleep(6);
    std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
    //获取所有raft节点ip、port ，并进行连接  ,要排除自己
    MprpcConfig config;
    config.LoadConfigFile(nodeInforFileName.c_str());
    std::vector<std::pair<std::string, short>> ipPortVt;
        for (int i = 0; i < INT_MAX - 1; ++i) {
            std::string node = "node" + std::to_string(i);

            std::string nodeIp = config.Load(node + "ip");
            std::string nodePortStr = config.Load(node + "port");
            if (nodeIp.empty()) {
            break;
        }
        ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));
    }
    std::vector<std::shared_ptr<RaftRpcUtil>> servers;
    //进行连接
    for (int i = 0; i < ipPortVt.size(); ++i) {
        // 1. 如果是“我自己”，就跳过
        // 因为我不需要通过网络给自己发 RPC，我自己调用自己函数就行
        if (i == m_me) {
            servers.push_back(nullptr);
            continue;
        }
        // 2. 如果是“别人”，就创建一个 RPC 客户端 (Stub)
        // 以后 m_raftNode->sendRequestVote 就会调用这个对象的方法
        std::string otherNodeIp = ipPortVt[i].first;
        short otherNodePort = ipPortVt[i].second;
        auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
        servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

        std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
    }
    sleep(ipPortVt.size() - me);  //等待所有节点相互连接成功，再启动raft
    m_raftNode->init(servers, m_me, persister, applyChan);
    // kv的server直接与raft通信，但kv不直接与raft通信，所以需要把ApplyMsg的chan传递下去用于通信，两者的persist也是共用的

    //////////////////////////////////
    // You may need initialization code here.
    // m_kvDB; //kvdb初始化
    m_skipList;
    waitApplyCh;
    m_lastRequestId;
    m_lastSnapShotRaftLogIndex = 0;  
    auto snapshot = persister->ReadSnapshot();
    if (!snapshot.empty()) {
        ReadSnapShotToInstall(snapshot);
    }
    std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this); 
    t2.join();  //由於ReadRaftApplyCommandLoop一直不會結束，达到一直卡在这的目的
}
