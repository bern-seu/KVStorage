#ifndef APPLYMSG_H
#define APPLYMSG_H
#include <string>
/* 
这个 ApplyMsg 类是整个系统架构中 Raft 层（底层） 和 KV应用层（上层） 之间沟通的桥梁
Raft 达成共识后，会通过一个 "通道"通知上层。这个 ApplyMsg 就是在那个通道里传输的数据包。
模式一：普通日志提交, 当 CommandValid == true 时，Raft 告诉 KVServer：“大家已经达成共识了，请执行这条命令。”
模式二：快照安装,当 SnapshotValid == true 时，Raft 告诉 KVServer：“直接清空当前内存数据,用这份全新的数据覆盖你的内存吧。”
*/
class ApplyMsg
{
public:
    //两个valid最开始要赋予false！！
  ApplyMsg()
      : CommandValid(false),
        Command(),
        CommandIndex(-1),
        SnapshotValid(false),
        SnapshotTerm(-1),
        SnapshotIndex(-1){

        }
public:
    bool CommandValid;
    std::string Command;
    int CommandIndex;
    bool SnapshotValid;
    std::string Snapshot;
    //快照对应的最后一条日志的索引和任期
    int SnapshotTerm;
    int SnapshotIndex;
};

#endif  // APPLYMSG_H