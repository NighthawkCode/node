#include "node/nodemaster_utils.h"

#include "node/nodelib.h"
#include "node/nodemaster.h"
#include "vlog.h"

namespace node {
void LaunchNodemasterOnSeparateThread(const char* data_path, const char* peerlist, ThreadInfo& ti) {
  constexpr const char* kNodemaster = "nodemaster";
  vlog_always(
      "[LaunchNodemasterOnSeparateThread] Launching nodemaster in threaded mode with data path: %s and "
      "nodemaster peers: %s",
      data_path, peerlist);
  ti.handle = new std::thread(node::nodemaster_main, data_path, peerlist, DEFAULT_NODE_REGISTRY_PORT);
  pthread_setname_np(ti.handle->native_handle(), kNodemaster);
  ti.end_fn = node::nodemaster_main_quit;

  // Wait until the node registry is listening for connections
  constexpr double TIMEOUT_SEC = 10.0;
  double start_time = time_now();
  while (node::check_master() != node::NodeError::SUCCESS) {
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(100ms);
    if (time_now() - start_time >= TIMEOUT_SEC) {
      vlog_error(VCAT_GENERAL,
                 "[LaunchNodemasterOnSeparateThread] Timed out waiting for the node registry to come online");
      ti.success = false;
      if (ti.handle) {
        ti.end_fn();
        if (ti.handle->joinable()) {
          ti.handle->join();
        }
        ti.handle = nullptr;
      }
    }
  }

  vlog_debug(VCAT_GENERAL,
             "[LaunchNodemasterOnSeparateThread] Started nodemaster with params: data path [%s], peers [%s]",
             data_path, peerlist);
  ti.success = true;
  ti.name = kNodemaster;
}

ThreadInfo::~ThreadInfo() {
  end_fn();
  if (handle) {
    vlog_debug(VCAT_GENERAL, "[ThreadInfo] Stopping thread '%s'... ", name.c_str());
    if (handle->joinable()) {
      handle->join();
    }
  }
}
}  // namespace node
