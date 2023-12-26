#pragma once

#include <cmath>
#include <functional>
#include <list>
#include <memory>
#include <unordered_map>
#include <vector>

#include "node/core.h"
#include "vlog.h"

// Queuing/blocking policy for subscriptions.
//
//  Policy Name       Block     Get LATEST / NEXT   Return REPEATs?
// -------------   ----------  ------------------- -----------------
//
//   POLL_NEXT         NO            NEXT              NO
//   -- N/A ---        NO            NEXT              YES
//  POLL_NEWEST        NO           LATEST             NO
//  READ_NEWEST        NO           LATEST             YES
//  BLOCK_NEXT         YES           NEXT              NO
//   -- N/A ---        YES           NEXT              YES
//  BLOCK_NEWEST       YES          LATEST             NO
//   -- N/A ---        YES          LATEST             YES
//
//  Here is how to interpret that table:
//    Block: the subscription will block, wait (with a timeout) for a
//           message to come.
//    Latest/New: Whether the subscription will return the next message in
//           chronological order or get the most recently published. Useful for
//           very slow subscribers, or not in sync with the publisher.
//    Repeats: Is it ok to return the same message more than once?
namespace node {

enum class SubscriptionPolicy {
  POLL_NEXT,
  POLL_NEWEST,
  READ_NEWEST,
  BLOCK_NEXT,
  BLOCK_NEWEST,
};

}

#include "node/node_app_helpers.h"

namespace node {

// This is the amount of time you want to wait while trying to connecto to a publisher
// This is a LARGE amount of time. If you have real-time nodes choose a smaller timeout
constexpr float NODE_APP_DEFAULT_CONNECT_TIMEOUT_SEC = 1.f;     // 1s
constexpr float NODE_APP_MINIMAL_CONNECT_TIMEOUT_SEC = 0.002f;  // 2ms

// The NodeApp class exposes an interface which creates publishers,
// that need a timeout value that decides when a message transmission fails. This is
// currently only used with network publishers.
// 1Gbps network speed means we can send 125kBytes per ms. For MOST cases we should
// choose the LOW setting. If you are transmitting images of the highest quality or sending
// large amounts of data embedded in your jpeg like the tile_forwarding_node or the hoop_forwarding_node
// you might want a higher timeout like 5 or 10 ms. Note choosing such high timeout
// will deteriorate your node's cycle rate if it's being published in the node's main run loop.
// LOW = Your message is smaller than 250kBytes
// MEDIUM = Your message is smaller than 1MBytes (Tile Forwarding nodes, Jpeg Node (Analyzer mode))
// HIGH = Your message is smaller than 5Mbytes
// VERY_HIGH = Your message is smaller than 10Mbytes (Hoop forwarding node)
// I am giving twice the amount of time necessary, just to be sure.
enum PublisherTimeoutMs { LOW = 4, MEDIUM = 16, HIGH = 80, VERY_HIGH = 160 };

// use this function if you want to set an accurate timeout for the publisher. be conservative
// and give yourself some room (20-30%) when you provide the buffer_size_bytes.
constexpr size_t gbps = 1024 * 1024 * 1024;
constexpr int ComputePublisherTimeout(size_t buffer_size_bytes, size_t bandwidth_bits_sec = gbps) {
  // compute the time it takes to publish these number of bytes in ms
  float bw_bytes_sec = (float(bandwidth_bits_sec) / 8.f);
  float time_ms = (float(buffer_size_bytes * 1000) / bw_bytes_sec);
  // ceil the float to an int
  return static_cast<int>(std::ceil(time_ms));
}

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
    UNKNOWN,       // Really unknown.
    INITIALIZING,  // Receiving messages, but not fully operational
    WARMUP,        // The node is initialized but still not fully running
    RUNNING,       // Nominal operating state
    TEARDOWN,      // Preparing to exit
    FAILED,        // Failure encountered
    FINISHED       // Finished nominally
  };

  enum NodeAppMode {
    MULTI_PROCESS,   // Default: each node is a process
    SINGLE_PROCESS,  // Running in single process mode (debug)
    SINGLE_THREAD    // Running all handlers in single thread (debug)
  };

  struct Options {
    NodeAppMode mode;               // Which mode are we running in?
    void* shared_config = nullptr;  // Shared configuration
    void (*warmup_complete_cb)() = nullptr;
  };

  // A soft periodic callback which will be called if elapsed time is
  // greater than the period, as checked during the main loop.
  // No hard-realtime guarantee.
  struct SoftPeriodicCallback {
    double period_sec_ = -1.0;
    double last_call_time_sec_ = -1.0;
    std::function<void()> callback_;

    SoftPeriodicCallback() = default;
    SoftPeriodicCallback(double period_sec, std::function<void()> callback)
        : period_sec_(period_sec)
        , last_call_time_sec_(time_now())
        , callback_(callback) {}

    void CallIfReady(double t_now_sec) {
      if (t_now_sec - last_call_time_sec_ > period_sec_) {
        callback_();
        last_call_time_sec_ = t_now_sec;
      }
    }
  };

  // Constructor.  Defaults to MULTI_PROCESS mode.
  NodeApp(const std::string& name, NodeAppMode mode = MULTI_PROCESS);

  // Destructor.
  virtual ~NodeApp();

  // Return the current state of the application.
  NodeAppState GetState() const { return state_; }
  const char* GetStateStr() const;

  // Return the current mode of the application (which can't
  // be changed after initialization).
  NodeAppMode GetMode() const { return mode_; }

  // Return the name of the node.
  const std::string& GetName() const { return name_; }

  // Set the name during initialization (e.g if overriding
  // from configuration file).  Can only be called during INITIALIZING state.
  void SetName(const std::string& name);

  void SetNotificationTimeoutMs(int notification_timeout_ms) {
    notification_timeout_ms_ = notification_timeout_ms;
  }

  bool IsFinished() const { return GetState() == FINISHED; }

  bool IsFailed() const { return GetState() == FAILED; }

  // This function must be implemented by derived classes in order
  // to clean up the node. Set the state to finished
  virtual void Finalize() = 0;

  // Derived classes can override this to do any cleanup actions.
  virtual void Abort() {}

  // Optional update function for non-message-driven interactions.
  virtual void Update() {}

  // Called when entering the RUNNING state.
  virtual void RunningHook() {}

  // Called when starting shutdown process.
  virtual void ShutDownHook() {}

  // Add a callback that will be polled and called if elapsed time is greater than period.
  // Callbacks are only called in INITIALIZING, WARMUP, and RUNNING states, not once
  // the node has failed or has begun shutting down.
  void AddPeriodicCallback(double period, std::function<void()> callback) {
    periodic_callbacks_.emplace_back(period, callback);
  }

  // Retrieve the notification manager, this allows libraries to piggyback on notifications
  node::NotificationManager& getNotificationManager() { return nm_; }

  // Loop over all subscriptions once, calling handlers for each
  // subscription.
  virtual bool DispatchMessages();

  // Returns true if the node is shutting down or has shut down.  For use by node_app_main.
  bool IsEnding() const;

  bool ResetSusbcription(const std::string& topic_name);

  virtual void CheckSubscriptions();

  virtual void CheckPeriodicCallbacks();

  // Subscribe to a message.  Type of handler should be:
  // std::function<void(MsgPtr<TMsg>& ptr)>
  template <typename TMsg>
  int Subscribe(MessageHandlerFun<TMsg> h, const std::string& topic,
                SubscriptionPolicy policy = SubscriptionPolicy::BLOCK_NEXT,
                float msg_timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC,
                float connect_timeout_sec = NODE_WAIT_FOREVER,
                std::shared_ptr<PendingSubscriptionBase> ps = nullptr) {
    std::unique_ptr<node::subscriber<TMsg>> sub = core_.subscribe<TMsg>(topic);
    sub->set_node_name(name_);
    if (policy == SubscriptionPolicy::POLL_NEXT) {
      sub->set_receive_next_message();
    }
    // connecting to nodemaster should not take any time at all. it's local
    // however what you actually want is to connect to the publisher under the given
    // time bounds
    auto res = sub->open_notify(nm_, h, 0, connect_timeout_sec);

    if (res == NodeError::SUCCESS) {
      Subscription<TMsg>* subscription =
          new Subscription<TMsg>(h, std::move(sub), msg_timeout_sec, policy, ps);
      subscriptions_.emplace_back(subscription);
      if (GetState() == RUNNING && !subscription->sub->is_network()) {
        // For SHM topics, there could be an already published message, try to consume it here
        // the notification manager would only do the callback for new messages, but only
        // after initialization
        if (subscription->sub->is_there_new()) {
          subscription->sub->process_message_notification(0);
        }
      } else {
        // In this case, we were still on an initialization state and we do not consume messages until we are
        // running
      }
    } else if (res == NodeError::HASH_MISMATCH || res == NodeError::TOPIC_TYPE_MISMATCH) {
      vlog_error(VCAT_GENERAL, "**** ERROR, mismatch of types on topic %s: %s!!!!", topic.c_str(),
                 NodeErrorToStr(res));
      return -1;
    } else {
      return -1;
    }

    return int(subscriptions_.size()) - 1;
  }

  // Subscribe to a message.  Type of handler should be:
  // void MyNodeApp::MessageHandler(node::MsgPtr<msg_type> msg_ptr);
  template <typename TApp, typename TMsg>
  int Subscribe(MessagePtrHandler<TApp, TMsg> h, const std::string& topic,
                SubscriptionPolicy policy = SubscriptionPolicy::BLOCK_NEXT,
                float msg_timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC,
                float connect_timeout_sec = NODE_WAIT_FOREVER,
                std::shared_ptr<PendingSubscriptionBase> ps = nullptr) {
    using namespace std::placeholders;
    MessageHandlerFun<TMsg> fn = std::bind(h, dynamic_cast<TApp*>(this), _1);
    return Subscribe(fn, topic, policy, msg_timeout_sec, connect_timeout_sec, ps);
  }

  // Returns list of publishers in 'pub'.
  void GetPublishers(std::vector<const publisher_base*>& pub) const;

  size_t SubscriptionCount() const { return subscriptions_.size(); }

  void handleExistingMessages();
  void setWarmupCompleteCallback(void (*cb)()) { this->warmup_complete_cb_ = cb; }

  // Sets the new state of the app.  This is used to
  // allow for hooks to be run in base class on state change.
  // If overriding this, call the base class's SetState method
  // in the override.  This is protected since state changes
  // should be triggered by events, rather than users poking
  // it.
  virtual void SetState(NodeAppState new_state);

  // Prefixes a topic.  By default this does nothing, but derived classes
  // may change this to indicate hostname if needed.
  virtual std::string PrefixTopic(const std::string& topic, bool network);

  template <typename TMsg>
  node::publisher<TMsg>* CreateChannel(const std::string& topic, u8 variant, bool network) {
    node::publisher<TMsg>* channel = new node::publisher<TMsg>;
    std::string final_topic = PrefixTopic(topic, network);
    *channel = core_.provides<TMsg>(final_topic);
    channel->set_message_name(TMsg::TYPE_STRING);
    channel->set_message_hash(TMsg::TYPE_HASH);
    channel->set_topic_variant(variant);
    return channel;
  }

  template <typename TMsg>
  node::publisher<TMsg>* ProvidesSHM(const std::string& topic, int num_messages = 10, u8 variant = 0) {
    static_assert(TMsg::is_simple(), "SHM topics can only be simple types");
    auto channel = CreateChannel<TMsg>(topic, variant, false);
    node::NodeError res = channel->open(num_messages);
    VLOG_ASSERT(res == node::SUCCESS, "[%s] Couldn't open channel with topic %s: result = %s", name_.c_str(),
                topic.c_str(), NodeErrorToStr(res));
    node::publisher_base* base = dynamic_cast<node::publisher_base*>(channel);
    VLOG_ASSERT(base != nullptr);
    publishers_[typeid(TMsg).hash_code()][topic].reset(base);
    return channel;
  }

  template <typename TMsg>
  node::publisher<TMsg>* ProvidesNetwork(const std::string& topic, int num_messages = 10, u8 variant = 0,
                                         int timeout_ms = node::PublisherTimeoutMs::LOW) {
    VLOG_ASSERT(timeout_ms > 0, "The timeout argument must be a positive number");
    if (timeout_ms > 10000) {
      vlog_warning(VCAT_NODE, "The timeout_ms is too large for a node. Might cause latencies in your node");
    }
    auto channel = CreateChannel<TMsg>(topic, variant, true);
    node::NodeError res = channel->open(num_messages, true, timeout_ms);
    VLOG_ASSERT(res == node::SUCCESS, "[%s] Couldn't open channel with topic %s: result = %s", name_.c_str(),
                topic.c_str(), NodeErrorToStr(res));
    node::publisher_base* base = dynamic_cast<node::publisher_base*>(channel);
    VLOG_ASSERT(base != nullptr);
    publishers_[typeid(TMsg).hash_code()][topic].reset(base);
    return channel;
  }

  // Return a message that can be published.  If there's only one topic
  // for the given message type, the topic name doesn't need to be supplied.
  template <typename TMsg>
  TMsg* PrepareMessage(const std::string& topic = "") {
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
  void TransmitMessage(TMsg*& msg, const std::string& topic = "") {
    VLOG_ASSERT(msg != nullptr);
    GetPublisher<TMsg>(topic)->transmit_message(msg);
    msg = nullptr;
  }

  // This is a more explicit form of TransmitMessage which should be used
  // to publish messages on topics that were declared as being shared memory
  // topics. It will fatal other wise. This is to promote users to think about
  // what effect their transmit will have and enhance readability of code.
  // Prefer using this over TransmitMessage.
  template <typename TMsg>
  void TransmitMessageSHM(TMsg*& msg, const std::string& topic = "") {
    VLOG_ASSERT(msg != nullptr);
    auto pub = GetPublisher<TMsg>(topic);
    if (pub->get_network()) {
      vlog_fatal(VCAT_NODE,
                 "The publisher for topic: %s and message type: %s is not a shared memory publisher",
                 topic.c_str(), pub->message_name.c_str());
    }
    pub->transmit_message(msg);
  }

  // This is a more explicit form of TransmitMessage which should be used
  // to publish messages on topics that were declared as being over the network
  // topics. It will fatal other wise. This is to promote users to think about
  // what effect their transmit will have and enhance readability of code. This transmit
  // CAN return false when we are on unreliable network. It is upto to the user to handle that failure.
  // Highly prefer using this over TransmitMessage.
  template <typename TMsg>
  [[nodiscard]] bool TransmitMessageNetwork(TMsg* msg, const std::string& topic = "") {
    VLOG_ASSERT(msg != nullptr);
    auto pub = GetPublisher<TMsg>(topic);
    if (!pub->get_network()) {
      vlog_fatal(VCAT_NODE,
                 "The publisher for topic: %s and message type: %s is not a network memory publisher",
                 topic.c_str(), pub->get_message_name().c_str());
    }
    return pub->transmit_message(msg);
  }

  template <typename TMsg>
  struct PendingSubscription : public PendingSubscriptionBase {
    MessageHandlerFun<TMsg> h_;
    PendingSubscription(MessageHandlerFun<TMsg> h, const std::string& t, SubscriptionPolicy p, float m,
                        float c)
        : PendingSubscriptionBase(t, p, m, c)
        , h_(h) {}
    ~PendingSubscription() override {}
    int ReSubscribe(NodeApp* p, std::shared_ptr<PendingSubscriptionBase> ps) override {
      return p->Subscribe(h_, topic, policy, msg_timeout_sec, connect_timeout_sec, ps);
    }
  };

  // VC-4104
  // Example 1. use std::bind()
  // using namespace std::placeholders;
  // AutoSubscribe( std::bind(Your::Function, thisptr, _1), ... );
  // Example 2. use lambda
  // AutoSubscribe( [this](node::MsgPtr<messages::yourmsg>& msg) { this->Function(msg); }, ... );
  template <typename TMsg>
  int AutoSubscribe(MessageHandlerFun<TMsg> h, const std::string& topic,
                    SubscriptionPolicy policy = SubscriptionPolicy::BLOCK_NEXT,
                    float msg_timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC,
                    float connect_timeout_sec = NODE_APP_DEFAULT_CONNECT_TIMEOUT_SEC) {
    auto pending_sub = std::shared_ptr<node::PendingSubscriptionBase>(
        new PendingSubscription<TMsg>(h, topic, policy, msg_timeout_sec, connect_timeout_sec));
    int res = Subscribe(h, topic, policy, msg_timeout_sec, connect_timeout_sec, pending_sub);
    if (-1 == res) {  // add pending subscription
      pending_subscriptions_.push_back(pending_sub);
    }
    return res;
  }

  template <typename TApp, typename TMsg>
  int AutoSubscribe(MessagePtrHandler<TApp, TMsg> h, const std::string& topic,
                    SubscriptionPolicy policy = SubscriptionPolicy::BLOCK_NEXT,
                    float msg_timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC,
                    float connect_timeout_sec = NODE_APP_DEFAULT_CONNECT_TIMEOUT_SEC) {
    using namespace std::placeholders;
    MessageHandlerFun<TMsg> fn = std::bind(h, dynamic_cast<TApp*>(this), _1);
    return AutoSubscribe(fn, topic, policy, msg_timeout_sec, connect_timeout_sec);
  }

  // Check if the topic is already subscribed to.  Do not subscribe more than once to a topic.
  bool IsTopicSubscribed(const std::string& topic);

  static bool IsFieldMode();

protected:
  template <typename TMsg>
  node::publisher<TMsg>* GetPublisher(const std::string& topic) {
    auto& topic_map = publishers_[typeid(TMsg).hash_code()];
    node::publisher_base* base;
    if (topic.empty() && topic_map.size() == 1) {
      const auto it = topic_map.begin();
      VLOG_ASSERT(it != topic_map.end());
      base = it->second.get();
    } else {
      base = topic_map[topic].get();
    }
    VLOG_ASSERT(base != nullptr, "topic: %s", topic.c_str());
    publisher<TMsg>* pub = dynamic_cast<publisher<TMsg>*>(base);
    VLOG_ASSERT(pub != nullptr, "topic: %s", topic.c_str());
    return pub;
  }

  node::core core_;
  node::NotificationManager nm_;

  // List of subscriptions to manage
  std::vector<std::unique_ptr<node::SubscriptionBase>> subscriptions_;

  // Auto-ReSubscribe if after the publisher did not exist upon Subscribe.
  std::list<std::shared_ptr<node::PendingSubscriptionBase>> pending_subscriptions_;

  // Double-keyed of publishers to manage. First key is message type,
  // second is topic.
  std::unordered_map<size_t,                          // First key - Message type (typeid().hash_code())
                     std::unordered_map<std::string,  // Second key - topic
                                        std::unique_ptr<node::publisher_base>>>
      publishers_;

  // Periodic callbacks, polled during main loop.  One of these is used for checking
  // subscriptions.
  std::vector<SoftPeriodicCallback> periodic_callbacks_;

  int notification_timeout_ms_ = 1000;

private:
  // Name of the app.
  std::string name_;

  // Which mode the app is running in.
  NodeAppMode mode_;

  // Which state the app is running in.
  NodeAppState state_ = UNKNOWN;

  // Callback to after warming up
  void (*warmup_complete_cb_)() = nullptr;
};

// Create a NodeApp.  This top-level function must be defined by the
// client in order, returning the singleton NodeApp to run.  If there is an
// unrecoverable failure during initialization, return a null pointer.
//
// NOTE: This is in the 'node' namespace so include that in your definition.
std::unique_ptr<NodeApp> InitializeNodeApp(int argc, char* argv[], const NodeApp::Options& options);

}  // namespace node
