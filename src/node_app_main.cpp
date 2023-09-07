#include <signal.h>

#include <cassert>

#include "node/node_app.h"

using namespace node;

static bool finish_app = false;
static void* shared_config = nullptr;
static bool app_is_ending = false;

// Function for launcher to use to end the node main loop
extern "C" void FinishNodeApp() {
  vlog_debug(VCAT_GENERAL, "Received message to stop, inner");
  finish_app = true;
  app_is_ending = true;
}

// Function for launcher to query if the app has begun shutdown.  Used for clean exit.
extern "C" bool IsNodeAppQuitting() { return app_is_ending; }

extern "C" void SetSharedConfig(void* sharedConfig) {
  if (sharedConfig) {
    vlog_debug(VCAT_GENERAL, "Using shared configuration");
    shared_config = sharedConfig;
  }
}

static void (*warmup_complete_cb)() = nullptr;

extern "C" void SetWarmupCompletedCb(void (*cb)()) { warmup_complete_cb = cb; }

static void sig_handler(int signo) {
  if (signo == SIGUSR1) {
    vlog_info(VCAT_GENERAL, "Received message to stop Node");
    FinishNodeApp();
  }
}

int main(int argc, char** argv) {
  NodeApp::Options options;
  std::unique_ptr<NodeApp> app;
  options.mode = NodeApp::MULTI_PROCESS;
  options.shared_config = shared_config;
  options.warmup_complete_cb = warmup_complete_cb;

  app = node::InitializeNodeApp(argc, argv, options);

  if (app == nullptr) {
    vlog_fatal(VCAT_GENERAL, "Node initialization failed for %s", argv[0]);
    return -1;
  }

  assert(app != nullptr);
  NVTX_NAME_THREAD(GetThreadId(), app->GetName().c_str());

  // Handle all the subscriptions of early messages
  app->handleExistingMessages();

  // Setup signal handlers
  struct sigaction old_sig = {};
  sigaction(SIGUSR1, nullptr, &old_sig);
  // If the handler is default or disabled, register it
  if ((old_sig.sa_handler == SIG_DFL) || (old_sig.sa_handler == SIG_IGN)) {
    struct sigaction new_action = {};
    sigemptyset(&new_action.sa_mask);
    new_action.sa_flags = 0;
    new_action.sa_handler = sig_handler;
    if (sigaction(SIGUSR1, &new_action, nullptr) == -1) {
      vlog_warning(VCAT_GENERAL, "Could not register the signal handler");
    }
  }

  while (!app->IsFinished() && !app->IsFailed() && !finish_app) {
    NVTX_RANGE_PUSH("DispatchMessages");
    app->DispatchMessages();
    NVTX_RANGE_POP();
    NVTX_RANGE_PUSH("Update");
    app->Update();
    if (app->IsEnding()) {
      app_is_ending = true;
    }
    NVTX_RANGE_POP();
    sched_yield();
  }

  if (app->IsFailed()) {
    vlog_error(VCAT_GENERAL, "NodeApp %s exiting due to failure.", app->GetName().c_str());
  } else if (app->IsFinished()) {
    vlog_info(VCAT_GENERAL, "NodeApp finished.");
  }

  // Ensure nodes clean up, if we reached here app was a valid pointer
  app->Finalize();
  return (app->GetState() == NodeApp::FINISHED ? 0 : -app->GetState());
}
