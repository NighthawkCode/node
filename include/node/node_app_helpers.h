#pragma once
#include "node/core.h"

namespace node {

struct PendingSubscriptionBase;

// This is a base class for a wrapper around node::subscription that
// provides for callback registration and message allocation/deallocation.
// Each message type has a templated subclass of this.
class SubscriptionBase {
public:
  virtual ~SubscriptionBase() {}

  SubscriptionPolicy policy_;
  virtual const std::string_view GetName() const = 0;
  virtual size_t GetNumMessagesPublished() const = 0;
  virtual size_t GetNumMessagesAvailable() const = 0;
  virtual size_t GetMaxMessagesAvailable() const = 0;
  virtual bool IsAlive() const = 0;
  virtual bool IsNetwork() const = 0;
  virtual void DebugStr(std::string& dbg) = 0;

  friend class NodeApp;

protected:
  bool enabled_ = true;
  float timeout_sec_ = 0.0;
  std::shared_ptr<PendingSubscriptionBase> pending_sub_;

  virtual NodeError CheckAndHandleMessage() = 0;
  virtual bool PollAndHandleMessage() = 0;
  virtual void ResetToNewest() = 0;

  SubscriptionBase(SubscriptionPolicy policy, float timeout_sec, std::shared_ptr<PendingSubscriptionBase> ps)
      : policy_(policy)
      , timeout_sec_(timeout_sec)
      , pending_sub_(ps) {}

  void Pause() { enabled_ = false; }
  void Resume() { enabled_ = true; }
  void SetTimeout(float timeout_sec) { timeout_sec_ = timeout_sec; }
  virtual void Close() = 0;

  // Constructor for MsgPtr.
  template <typename T>
  MsgPtr<T> make_msg_ptr(T* ptr, node::subscriber<T>* sub) {
    return MsgPtr<T>(ptr, sub);
  }
};

// Handler callback function pointer.
template <typename TApp, typename TMsg>
using MessagePtrHandler = void (TApp::*)(MsgPtr<TMsg>& ptr);

template <typename TMsg>
using MessageHandlerFun = std::function<void(MsgPtr<TMsg>& ptr)>;

// Subscription for a message--combination of subscriber<T> and handler,
// along with polling functions.
// Users of NodeApp don't need to access this directly.
template <typename TMsg>
class Subscription : public SubscriptionBase {
  friend class NodeApp;
  const std::string_view GetName() const override { return sub->topic_name(); }
  size_t GetNumMessagesPublished() const override { return sub->get_num_published(); }
  size_t GetNumMessagesAvailable() const override { return sub->get_num_available(); }
  size_t GetMaxMessagesAvailable() const override { return sub->get_max_available(); }
  bool IsAlive() const override { return sub->is_alive(); }
  void Close() override { sub->close(); }

protected:
  MessageHandlerFun<TMsg> handler;
  std::unique_ptr<node::subscriber<TMsg>> sub;

  Subscription(MessageHandlerFun<TMsg> h, std::unique_ptr<node::subscriber<TMsg>>&& s, float poll_timeout_sec,
               SubscriptionPolicy policy, std::shared_ptr<PendingSubscriptionBase> ps)
      : SubscriptionBase(policy, poll_timeout_sec, ps)
      , handler(h)
      , sub(std::move(s)) {}

  // Check for messages and dispatch message to handler.
  NodeError CheckAndHandleMessage() override {
    NodeError res;
    MsgPtr<TMsg> msg;
    switch (policy_) {
      case SubscriptionPolicy::POLL_NEXT:
        sub->consume_message_once();
        if (sub->is_there_new()) {
          msg = sub->get_message(res);
        } else {
          return NodeError::CONSUMER_TIME_OUT;
        }
        break;
      case SubscriptionPolicy::POLL_NEWEST:
        sub->consume_message_once();
        if (sub->is_there_new()) {
          sub->reset_message_index();
          msg = sub->get_message(res);
        } else {
          return NodeError::CONSUMER_TIME_OUT;
        }
        break;
      case SubscriptionPolicy::READ_NEWEST:
        sub->reset_message_index();
        if (sub->is_there_new()) {
          msg = sub->get_message(res);
        } else {
          return NodeError::CONSUMER_TIME_OUT;
        }
        break;
      case SubscriptionPolicy::BLOCK_NEXT:
        sub->consume_message_once();
        msg = sub->get_message(res, timeout_sec_);
        break;
      case SubscriptionPolicy::BLOCK_NEWEST:
        sub->consume_message_once();
        if (!sub->is_there_new()) {
          msg = sub->get_message(res);
        } else {
          sub->reset_message_index();
          msg = sub->get_message(res);
        }
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
      NVTX_RANGE_PUSH("HandleMessage");
      handler(msg);
      NVTX_RANGE_POP();
    }
    return res;
  }

  // Peek at messages and dispatch message to handler,
  // focus on zero timeouts
  // Returns true if a message was processed, false otherwise
  bool PollAndHandleMessage() override {
    switch (policy_) {
      case SubscriptionPolicy::POLL_NEXT:
      case SubscriptionPolicy::BLOCK_NEXT:
        sub->consume_message_once();
        if (sub->is_there_new()) {
          return sub->process_message_notification(0);
        } else {
          return false;
        }
        break;
      case SubscriptionPolicy::POLL_NEWEST:
      case SubscriptionPolicy::BLOCK_NEWEST:
        sub->consume_message_once();
        if (sub->is_there_new()) {
          sub->reset_message_index();
          return sub->process_message_notification(0);
        } else {
          return false;
        }
        break;
      case SubscriptionPolicy::READ_NEWEST:
        vlog_error(VCAT_NODE, "Poll is not compatible with READ_NEWEST");
        return false;
        break;
      default:
        assert(false);
        break;
    }
    return false;
  }

  bool IsNetwork() const override { return sub->is_network(); }

  void ResetToNewest() override { sub->reset_message_index(); }

  void DebugStr(std::string& dbg) override { sub->debug_string(dbg); }
};

// Base class item for automatically re-scubscribing.
struct PendingSubscriptionBase {
  std::string topic;
  SubscriptionPolicy policy;
  float msg_timeout_sec;
  float connect_timeout_sec;
  PendingSubscriptionBase(const std::string& t, SubscriptionPolicy p, float m, float c)
      : topic(t)
      , policy(p)
      , msg_timeout_sec(m)
      , connect_timeout_sec(c) {}
  virtual ~PendingSubscriptionBase() {}
  virtual int ReSubscribe(class NodeApp* p, std::shared_ptr<PendingSubscriptionBase> ps) = 0;
};

}  // namespace node
