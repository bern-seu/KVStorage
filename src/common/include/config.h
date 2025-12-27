#ifndef CONFIG_H
#define CONFIG_H

const bool Debug = true;
// 协程相关设置
const int FIBER_THREAD_NUM = 1;              // 协程库中线程池大小
const bool FIBER_USE_CALLER_THREAD = false;  // 是否使用caller_thread执行调度任务

#endif  // CONFIG_H
