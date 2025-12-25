/* Raft算法流程 */
#ifndef RAFT_H
#define RAFT_H


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

#endif  // RAFT_H