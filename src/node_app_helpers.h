#pragma once
#include "node/core.h"

namespace node {

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
  
  virtual NodeError CheckAndHandleMessage() = 0;
  SubscriptionBase(float timeout_sec) : timeout_sec_(timeout_sec) {}

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
               node::subscriber<TMsg> &&s, float poll_timeout_sec) :
    owning_app(owner), handler(h), SubscriptionBase(poll_timeout_sec),
    sub(std::move(s)) {
  }

  // Check for messages and dispatch message to handler.
  NodeError CheckAndHandleMessage() override {
    NodeError res;
    MsgPtr<TMsg> msg = sub.get_message(res);
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
