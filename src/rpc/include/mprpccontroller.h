#pragma once
#include <google/protobuf/service.h>
#include <string>

class MprpcController : public google::protobuf::RpcController {
 public:
  MprpcController();
  void Reset();
  bool Failed() const;
  std::string ErrorText() const;
  void SetFailed(const std::string& reason);

  // 设置超时时间 (毫秒)
  void SetTimeout(int timeout_ms);
  // 获取超时时间
  int GetTimeout() const;

  // 目前未实现具体的功能
  void StartCancel();
  bool IsCanceled() const;
  void NotifyOnCancel(google::protobuf::Closure* callback);

 private:
  bool m_failed;          // RPC方法执行过程中的状态
  std::string m_errText;  // RPC方法执行过程中的错误信息
  // 超时时长，默认 -1 代表无超时
  int m_timeout_ms;
};