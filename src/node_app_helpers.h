#pragma once
#include "node/core.h"

namespace node {

enum class SubscriptionPolicy {
  MOST_RECENT,
  LATEST,
  WAIT_NEXT
};

// This is a base class for a wrapper around node::subscription that
// provides for callback registration and message allocation/deallocation.
// Each message type has a templated subclass of this.
class SubscriptionBase {
public:
  virtual ~SubscriptionBase() {}

friend class NodeApp;
protected:
  bool enabled_ = true;
  float timeout_sec_ = 0.0;
  SubscriptionPolicy policy_;

  
  virtual NodeError CheckAndHandleMessage() = 0;
  SubscriptionBase(SubscriptionPolicy policy, float timeout_sec) :
    policy_(policy), timeout_sec_(timeout_sec) {}

  void Pause() { enabled_ = false; }
  void Resume() { enabled_ = true; }
  void SetTimeout(float timeout_sec) { timeout_sec_ = timeout_sec; }

  // Constructor for MsgPtr.
  template <typename T>
  MsgPtr<T> make_msg_ptr(T *ptr, node::subscriber<T> *sub) {
    return MsgPtr<T>(ptr, sub);
  }
};

// Handler callback function pointer.
template <typename TApp, typename TMsg>
using MessagePtrHandler = void (TApp::*)(MsgPtr<TMsg> &ptr);

// Subscription for a message--combination of subscriber<T> and handler,
// along with polling functions.
// Users of NodeApp don't need to access this directly.
template <typename TApp, typename TMsg>
class Subscription : public SubscriptionBase {
friend class NodeApp;
protected:
  MessagePtrHandler<TApp, TMsg> handler = nullptr;
  TApp *owning_app = nullptr;
  node::subscriber<TMsg> sub;
  Subscription(TApp *owner, MessagePtrHandler<TApp, TMsg> h,
               node::subscriber<TMsg> &&s,
               float poll_timeout_sec,
               SubscriptionPolicy policy = SubscriptionPolicy::WAIT_NEXT) : 
    owning_app(owner), handler(h), SubscriptionBase(policy, poll_timeout_sec),
    sub(std::move(s)) {
  }

  // Check for messages and dispatch message to handler.
  NodeError CheckAndHandleMessage() override {
    NodeError res;
    // Optionally, accept a parameter to choose if to get the next, recent or latest message
    MsgPtr<TMsg> msg;
    switch(policy_) {
    case SubscriptionPolicy::MOST_RECENT:
      msg = sub.get_recent_message(res);
      break;
    case SubscriptionPolicy::LATEST:
      msg = sub.get_latest_message(res);
      break;
    case SubscriptionPolicy::WAIT_NEXT:
      msg = sub.get_next_message(res);
      break;
    default:
      assert(false);
      break;
    }
    if (res == SUCCESS) {
      assert(msg.get() != nullptr);
      assert(handler != nullptr);
      // Caller can transfer ownership to their own MsgPtr if they want to
      // maintain ownership of the message.  If they don't, it'll be
      // freed at the end of their handler.
      (owning_app->*handler)(msg);
    }
    return res;
  }
};

}
