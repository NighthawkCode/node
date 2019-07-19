#pragma once

namespace node {

//////////////////////////////////////////////////////////////////////
// NodeApp Wrapper for a Node-based application that standardizes
// initialization and message handling patterns to enable use in
// multi-process, single-process, and single-thread modes:
// - Multi-process is the normal operating mode; each Node is a process.
// - Single-process runs many NodeApps in a single process, with each Node
//   having its own thread.
// - Single-thread runs all Nodes serially in a single thread.  Each
//   Node may still use multiple threads internally, but incoming messages
//   are handled in a round-robin way between nodes.
//
// Programming model:
// - Define a subclass of NodeApp with application-specific logic.
//   - In the single-thread mode, the HandleMessage() function should
//     check all subscriptions in turn and handle the first message--and
//     only the first message--found.  If not messages are pending, it
//     should return.  It should not block on any message queues.
// - Create a static node::InitializeNodeApp function that allocates,
//   initializes, and returns a NodeApp object.  The mode the app
//   will be run in will be passed in the options.mode member.
// - All of this code should should be in a .so file.  To build the
//   standard binary, include the node_app_main.cpp file in the binary
//   target.
// - The single-process and single-thread binaries are standardized and
//   can load multiple NodeApp .so files in a standardized way at runtime.
//  
//////////////////////////////////////////////////////////////////////

  class NodeApp {
    enum NodeAppState {
      UNKNOWN,        // Really unknown.
      INITIALIZING,   // Receiving messages, but not fully operational
      RUNNING,        // Nominal operating state
      TEARDOWN,       // Preparing to exit
      FINISHED,       // Finished nominally
      FAILED          // Program failed.
    };

    enum NodeAppMode {
      MULTI_PROCESS,   // Default: each node is a process
      SINGLE_PROCESS,  // Running in single process mode (debug)
      SINGLE_THREAD    // Running all handlers in single thread (debug)
    };

    NodeAppOptions {
      NodeAppMode mode;  // Which mode are we running in?
    };

  public:
    // Constructor.  Defaults to MULTI_PROCESS mode.
    NodeApp(NodeAppMode mode = MULTI_PROCESS) :
      mode_(mode) {}

    // Check all subscriptions for messages, one time, and handle them.
    // Returns true if any messages were pending (and one was handled),
    // or false if not.
    virtual bool HandleMessage() = 0;

    // Return the current state of the application.
    virtual NodeAppState GetState() { return state };

  protected:
    // Which mode the app is running in.
    NodeAppMode mode_;

    // Which state the app is running in.
    NodeAppState state_ = UNKNOWN;
  };
  
  // Create a NodeApp.  This top-level function must be defined by the
  // client in order, returning the singleton NodeApp to run.
  NodeApp *InitializeNodeApp(int argc, char *argv, NodeAppOptions options);

} // namespace node
