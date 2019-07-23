#include <cstdlib>
#include <cstdio>
#include <cassert>
#include "node/node_app.h"

using namespace node;

int main(int argc, char *argv[]) {
  // Create a NodeApp, allowing it to parse the command-line arguments
  NodeApp::Options options;
  options.mode = NodeApp::MULTI_PROCESS;
  std::unique_ptr<NodeApp> app = node::InitializeNodeApp(argc, argv, options);

  if (app == nullptr) {
    fprintf(stderr, "Node initialization failed.");
    exit(-1);
  }
  assert(app != nullptr);
  while (!app->IsFinished()) {
    app->DispatchMessages();
    app->Update();
  }

  exit(app->GetState() == NodeApp::FINISHED ? 0 : -app->GetState());
}
