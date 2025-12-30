#include "raftRpcUtil.h"
#include "config.h"
#include <mprpcchannel.h>
#include <mprpccontroller.h>

bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response){
    MprpcController controller;
    controller.SetTimeout(minRandomizedElectionTime);
    stub_->AppendEntries(&controller, args, response, nullptr);
    return !controller.Failed();
}

bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                                  raftRpcProctoc::InstallSnapshotResponse *response) {
    MprpcController controller;
    controller.SetTimeout(kSnapshotTimeout);
    stub_->InstallSnapshot(&controller, args, response, nullptr);
    return !controller.Failed();
}

bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response) {
    MprpcController controller;
    controller.SetTimeout(minRandomizedElectionTime);
    stub_->RequestVote(&controller, args, response, nullptr);
    return !controller.Failed();
}

//先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动

RaftRpcUtil::RaftRpcUtil(std::string ip, short port) {
    //*********************************************  */
    //发送rpc设置
    // 1. 先创建 channel 并保存指针
    channel_ = new MprpcChannel(ip, port, true);
    // 2. 再创建 stub
    stub_ = new raftRpcProctoc::raftRpc_Stub(channel_);
}
RaftRpcUtil::~RaftRpcUtil() {
    delete stub_; 
    delete channel_; 
}
