#pragma once
#include <functional>
#include <string>
#include <thread>

namespace node {
struct ThreadInfo {
  std::thread* handle = nullptr;
  bool success = false;
  std::function<void()> end_fn = []() {};
  std::string name = "undefined";
  ~ThreadInfo();
};

void LaunchNodemasterOnSeparateThread(const char* data_path, const char* peerlist, ThreadInfo& ti);
}  // namespace node
