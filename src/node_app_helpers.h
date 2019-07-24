#pragma once
#include "node/core.h"

namespace node {

// Simple smart pointer for messages.  Subset of functionality of
// unique_ptr.  This allows NodeApps to hold onto a message outside their
// message handler.
template <typename Msg>  
class MsgPtr {
public:
  friend class SubscriptionBase;

  // Empty constructor.
  MsgPtr() {} 

  // Construct a new MsgPtr and transfer ownership 
  MsgPtr(MsgPtr &&rhs) {
    sub_ = rhs.sub_;
    ptr_ = rhs.ptr_;
    
    rhs.sub_ = nullptr;
    rhs.ptr_ = nullptr;
  }

  // Destructor.  Deallocates message.
  ~MsgPtr() {
    release();
  }

  // Accessor.
  Msg *get() { return ptr_; }

  // Dereference operator.
  Msg *operator->() const {
    assert(ptr_ != nullptr);
    return ptr_;
  }

  // Assignment operator - transfers ownership from RHS.  Use like this:
  // new_ptr = std::move(old_ptr);
  void operator=(MsgPtr &&rhs) {
    if (ptr_ != nullptr) {
      if (ptr_ == rhs.ptr_) {
        assert(sub_ == rhs.sub_);
        rhs.ptr_ = nullptr;
        rhs.sub_ = nullptr;
        return;
      }
      release();
    }
    sub_ = rhs.sub_;
    ptr_ = rhs.ptr_;
    
    rhs.sub_ = nullptr;
    rhs.ptr_ = nullptr;
  }

  // Releases ownership and deallocates message.
  void release() {
    if (ptr_ != nullptr) {
      assert(sub_ != nullptr);
      sub_->release_message(ptr_);
    }
    ptr_ = nullptr;
    sub_ = nullptr;
  }

protected:
  // Protected constructor to use by friendly Subscription class.
  MsgPtr(Msg *m, node::subscriber<Msg> *sub) : sub_(sub), ptr_(m) {
    assert(sub_ != nullptr);
    assert(ptr_ != nullptr);
  }

private:
  node::subscriber<Msg> *sub_ = nullptr;
  Msg *ptr_ = nullptr;
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
    TMsg *msg = sub.get_message(res);
    if (res == SUCCESS) {
      assert(msg != nullptr);
      assert(handler != nullptr);
      MsgPtr<TMsg> m = make_msg_ptr(msg, &sub);
      // Caller can transfer ownership to their own MsgPtr if they want to
      // maintain ownership of the message.  If they don't, it'll be
      // freed at the end of their handler.
      (owning_app->*handler)(m);
    }
    return res;
  }
};

}
