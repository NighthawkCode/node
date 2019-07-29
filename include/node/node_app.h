#pragma once
#include "node/core.h"
#include "node_app_helpers.h"

#include <unordered_map>
#include <memory>
#include <vector>
#include <functional>

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
//   - Use the Subscribe() method to subscribe handlers for messages and
//     topics, ideally in the constructor.
//   - Use the Provides<msg>() method to announce messages that will be
//     published (again, ideally in the constructor).
//   - If the app is not purely event-driven (i.e. if it's timer-driven or
//     is polling hardware), override the Update() method and put this
//     logic in there.
// - Create a static node::InitializeNodeApp function that allocates,
//   initializes, and returns a NodeApp object.  The mode the app
//   will be run in will be passed in the options.mode member.
// - All of this code should should be in a .so file.  To build the
//   standard binary, include the node_app_main.cpp file in the binary
//   target.
// - The single-process and single-thread binaries are standardized and
//   can load multiple NodeApp .so files in a standardized way at runtime.
//
// See test/prod_app.cpp and test/cons_app.cpp for simple examples.
//
// Brief example:
//
//   class MyNode : public NodeApp {
//   public:
//   MyNode() {
//     Provides<MyMessage>("my_topic");
//     Subscribe(MyHandler, "your_topic");
//   }  
//     
//   void MyHandler(MsgPtr<YourMessage> &msg) {
//      printf("Got your message.\n");
//      // do stuff with incoming message.  
//
//      // Publish a message
//      auto *my_msg = PrepareMessage<MyMessage>();
//      // populate my_msg
//      TransmitMessage(my_msg);
//   }
//
//   // This method is optional, if we need to be driven by other things
//   // besides incoming messages.  
//   void Update() override {
//      // Poll some hardware, if we need to
//      if (some_hardware_signal) {
//        SetState(FINISHED);  // when we're done and want to exit
//      }   
//   }  
//   };
//
//   // This should go in a .cpp file, not a .h file.  
//   std::unique_ptr<NodeApp> *InitializeNodeApp(int argc, char *argv,  
//                            const NodeApp::Options &options) {
//      // parse command-line options as needed...
//      return std::unique_ptr<NodeApp>(new MyNodeApp);
//   }
//////////////////////////////////////////////////////////////////////
class NodeApp {
public:
  enum NodeAppState {
    UNKNOWN,        // Really unknown.
    INITIALIZING,   // Receiving messages, but not fully operational
    RUNNING,        // Nominal operating state
    TEARDOWN,       // Preparing to exit
    FAILED,         // Failure encountered
    FINISHED        // Finished nominally
  };
  
  enum NodeAppMode {
    MULTI_PROCESS,   // Default: each node is a process
    SINGLE_PROCESS,  // Running in single process mode (debug)
    SINGLE_THREAD    // Running all handlers in single thread (debug)
  };
  
  struct Options {
    NodeAppMode mode;  // Which mode are we running in?
  };
  
  // Constructor.  Defaults to MULTI_PROCESS mode.
  NodeApp(NodeAppMode mode = MULTI_PROCESS) :
    mode_(mode) {}

  virtual ~NodeApp() {}
  
  // Return the current state of the application.
  NodeAppState GetState() const { return state_; }
  
  // Return the current mode of the application (which can't
  // be changed after initialization).
  NodeAppMode GetMode() const { return mode_; }

  bool IsFinished() const {
    return GetState() == FINISHED;
  }

  // Derived classes can override this to do any cleanup actions.
  virtual void Abort() {}

  // Optional update function for non-message-driven interactions.
  virtual void Update() {}

  // Loop over all subscriptions once, calling handlers for each
  // subscription.  
  virtual bool DispatchMessages() {
    bool dispatched = false;
    for (auto &s : subscriptions_) {
      if (s->enabled_) {
        NodeError res = s->CheckAndHandleMessage();
        if (res == SUCCESS) {
          dispatched = true;
        } else if (res != CONSUMER_TIME_OUT) {
          SetState(FAILED);
          Abort();
          return dispatched;
        }
      }
    }
    return dispatched;
  }

protected:
  // Sets the new state of the app.  This is used to
  // allow for hooks to be run in base class on state change.
  // If overriding this, call the base class's SetState method
  // in the override.  This is protected since state changes
  // should be triggered by events, rather than users poking
  // it.
  virtual void SetState(NodeAppState state) {
    state_ = state;
  };

  template<typename TMsg>
  node::publisher<TMsg> *Provides(const std::string &topic, int num_messages) {
    node::publisher<TMsg> *channel = new node::publisher<TMsg>;
    *channel = core_.provides<TMsg>(topic);
    node::NodeError res = channel->open(num_messages);
    assert(res == node::SUCCESS);
    node::publisher_base *base = dynamic_cast<node::publisher_base *>(channel);
    publishers_[typeid(TMsg).hash_code()][topic].reset(base);
    return channel;
  }

  // Return a message that can be published.  If there's only one topic
  // for the given message type, the topic name doesn't need to be supplied.
  template<typename TMsg>
  TMsg *PrepareMessage(const std::string &topic = "") {
    return GetPublisher<TMsg>(topic)->prepare_message();
  }

  // Transmits a message obtained from PrepareMessage().  Can only
  // be transmitted a single time, and 'msg' will be set to nullptr
  // to reflect this.
  //
  // If there's only one topic for the given message type, the
  // topic name doesn't need to be supplied.
  //
  // NOTE: Behavior is undefined if a message from one topic is published
  // on another.  Don't try that at home.
  template <typename TMsg>
  void TransmitMessage(TMsg* &msg, const std::string &topic = "") {
    assert(msg != nullptr);
    GetPublisher<TMsg>(topic)->transmit_message(msg);
    msg = nullptr;
  }

  // Subscribe to a message.  Type of handler should be:
  // void MyNodeApp::MessageHandler(node::MsgPtr<msg_type> msg_ptr);
  template <typename TApp, typename TMsg>
  int Subscribe(MessagePtrHandler<TApp, TMsg> h,
                const std::string &topic,
                SubscriptionPolicy policy = SubscriptionPolicy::WAIT_NEXT,
                float msg_timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC,
                float connect_timeout_sec = NODE_WAIT_FOREVER) {
    node::subscriber<TMsg> sub = core_.subscribe<TMsg>(topic);
    auto res = sub.open(connect_timeout_sec);

    if (res == NodeError::SUCCESS) {
      Subscription<TApp,TMsg> *subscription =
        new Subscription<TApp,TMsg>(dynamic_cast<TApp *>(this), h,
                                    std::move(sub),
                                    msg_timeout_sec, policy);
      subscriptions_.emplace_back(subscription);
    } else {
      SetState(FAILED);
      return -1;
    }

    return subscriptions_.size()-1;
  }

protected:
  template <typename TMsg>
  node::publisher<TMsg> *GetPublisher(const std::string &topic) {
    auto &topic_map = publishers_[typeid(TMsg).hash_code()];
    node::publisher_base *base; 
    if (topic.empty() && topic_map.size() == 1) {
      const auto it = topic_map.begin();
      assert(it != topic_map.end());
      base = it->second.get();
    } else {
      base = topic_map[topic].get();
    }
    assert(base != nullptr);
    publisher<TMsg> *pub = dynamic_cast<publisher<TMsg> *>(base);
    assert(pub != nullptr);
    return pub;
  }
  
  node::core core_;

  // List of subscriptions to manage
  std::vector<std::unique_ptr<node::SubscriptionBase>> subscriptions_;

  // Double-keyed of publishers to manage. First key is message type,
  // second is topic.
  std::unordered_map
  <size_t, // First key - Message type (typeid().hash_code())
   std::unordered_map<std::string, // Second key - topic
                      std::unique_ptr<node::publisher_base>>> publishers_;

private:
  // Which mode the app is running in.
  NodeAppMode mode_;
  
  // Which state the app is running in.
  NodeAppState state_ = UNKNOWN;
};
  
// Create a NodeApp.  This top-level function must be defined by the
// client in order, returning the singleton NodeApp to run.  If there is an
// unrecoverable failure during initialization, return a null pointer.
//
// NOTE: This is in the 'node' namespace so include that in your definition.  
std::unique_ptr<NodeApp> InitializeNodeApp(int argc, char *argv[],
                                           const NodeApp::Options &options);

} // namespace node

