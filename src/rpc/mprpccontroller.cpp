#include "mprpccontroller.h"

MprpcController::MprpcController() {
  m_failed = false;
  m_errText = "";
  m_timeout_ms = -1;
}

void MprpcController::Reset() {
  m_failed = false;
  m_errText = "";
  m_timeout_ms = -1;
}

bool MprpcController::Failed() const { return m_failed; }

std::string MprpcController::ErrorText() const { return m_errText; }

void MprpcController::SetFailed(const std::string& reason) {
  m_failed = true;
  m_errText = reason;
}
void MprpcController::SetTimeout(int timeout_ms) {
    m_timeout_ms = timeout_ms;
}

int MprpcController::GetTimeout() const {
    return m_timeout_ms;
}

// 目前未实现具体的功能
void MprpcController::StartCancel() {}
bool MprpcController::IsCanceled() const { return false; }
void MprpcController::NotifyOnCancel(google::protobuf::Closure* callback) {}