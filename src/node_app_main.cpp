#include <cstdlib>
#include <node_app.h>

using namespace node;

int main(int argc, char *argv[]) {
  // Create a NodeApp, allowing it to parse the command-line arguments
  NodeAppOptions options;
  options.mode = NodeApp::MULTI_PROCESS;
  NodeApp *app = InitializeNodeApp(argc, argv, options);

  assert(app != nullptr);
  while (app->GetState() != NodeApp::FINISHED) {
    app->HandleMessages();
  }

  exit(app->GetState() == NodeApp::FINISHED ? 0 : -app->GetState());
}
