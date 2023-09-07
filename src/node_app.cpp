#include "node/node_app.h"

namespace node {

bool NodeApp::IsTopicSubscribed(const std::string& topic) {
  for (auto& ptr : pending_subscriptions_) {
    if (ptr->topic == topic) {
      return true;
    }
  }
  for (auto& ptr : subscriptions_) {
    if (ptr->GetName() == topic) {
      return true;
    }
  }
  return false;
}

void NodeApp::handleExistingMessages() {
  for (auto& subscription : subscriptions_) {
    if (!subscription->IsNetwork()) {
      subscription->CheckAndHandleMessage();
    }
  }
}

const char* NodeApp::GetStateStr() const {
  switch (state_) {
    case INITIALIZING:
      return "INITIALIZING";
    case RUNNING:
      return "RUNNING";
    case TEARDOWN:
      return "TEARDOWN";
    case FAILED:
      return "FAILED";
    case FINISHED:
      return "FINISHED";
    case UNKNOWN:
      return "UNKNOWN";
    case WARMUP:
      return "WARMUP";
  }
  return "UNKNOWN";
}

// Constructor.  Defaults to MULTI_PROCESS mode.
NodeApp::NodeApp(const std::string& name, NodeAppMode mode)
    : name_(name)
    , mode_(mode) {
  // Add a periodic callback to check subscriptions every 2 seconds.
  // Note that this will not be called once we being the shutdown process.
  AddPeriodicCallback(2.0, [this]() { this->CheckSubscriptions(); });
  nm_.set_node_name(name);
}

NodeApp::~NodeApp() {
  // Close all publishers in advance
  for (auto& hash_and_map : publishers_) {
    for (auto& topic_and_pub : hash_and_map.second) {
      topic_and_pub.second->preclose();
    }
  }
}

void NodeApp::SetName(const std::string& name) {
  VLOG_ASSERT(GetState() == NodeAppState::INITIALIZING, "Can only change name during initialization");
  name_ = name;
  nm_.set_node_name(name);
}

// Loop over all subscriptions once, calling handlers for each
// subscription.
bool NodeApp::DispatchMessages() {
  bool dispatched = false;

  // Opportunistic, try to see if there is a message to handle now before we go to
  // notification manager
  for (auto& s : subscriptions_) {
    if (s->enabled_) {
      bool handled = s->PollAndHandleMessage();
      dispatched = dispatched || handled;
    }
  }

  if (!dispatched) {
    // Wait here for a new data to be published on any of the subscribers
    int wait_result = nm_.wait_for_notification(notification_timeout_ms_);
    dispatched = wait_result > 0;
    if (wait_result < 0) {
      SetState(FAILED);
      Abort();
    }
  }

  switch (state_) {
    case INITIALIZING:
    case WARMUP:
    case RUNNING:
      // Check for subscriptions
      for (auto s_it = subscriptions_.begin(); s_it != subscriptions_.end();) {
        auto& s = *s_it;
        if (s->enabled_) {
          if (!s->IsAlive()) {
            vlog_error(VCAT_GENERAL, "Subscription %s is no longer alive", std::string(s->GetName()).c_str());
            // remove this subscription, move it to auto if possible
            if (s->pending_sub_ != nullptr) {
              // if we are shutting down we don't want to keep checking for pending subscriptions
              pending_subscriptions_.push_back(s->pending_sub_);
              s_it = subscriptions_.erase(s_it);
              continue;
            }
          }
        }
        s_it++;
      }
      CheckPeriodicCallbacks();
      break;

    default:
      // Do not check for subscriptions in TEARDOWN, FAILED, FINALIZED
      // Do not check periodic callbacks either
      break;
  }
  return dispatched;
}

// Returns true if the node is shutting down or has shut down.  For use by node_app_main.
bool NodeApp::IsEnding() const {
  switch (state_) {
    case UNKNOWN:
    case INITIALIZING:
    case WARMUP:
    case RUNNING:
      return false;
    case TEARDOWN:
    case FAILED:
    case FINISHED:
      return true;
  }
}

bool NodeApp::ResetSusbcription(const std::string& topic_name) {
  for (auto s_it = subscriptions_.begin(); s_it != subscriptions_.end();) {
    auto& s = *s_it;
    // remove this subscription, move it to auto if possible
    if (s->GetName() == topic_name) {
      if (s->pending_sub_ != nullptr) {
        pending_subscriptions_.push_back(s->pending_sub_);
        s_it = subscriptions_.erase(s_it);
        return true;
      } else {
        vlog_error(VCAT_GENERAL, "Trying to reset subscription of topic %s but it was not setup correctly!",
                   topic_name.c_str());
        return false;
      }
    } else {
      s_it++;
    }
  }
  vlog_error(VCAT_GENERAL, "Trying to reset subscription of topic %s but it could not be found",
             topic_name.c_str());
  return false;
}

void NodeApp::CheckSubscriptions() {
  std::list<std::shared_ptr<node::PendingSubscriptionBase>>::iterator i = pending_subscriptions_.begin();
  for (; i != pending_subscriptions_.end();) {
    // why? because removal in loop messes iterators.
    std::list<std::shared_ptr<node::PendingSubscriptionBase>>::iterator inext = i;
    inext++;
    std::shared_ptr<node::PendingSubscriptionBase>& p = *i;
    i = inext;
    int res = p->ReSubscribe(this, p);
    if (res != -1) {
      pending_subscriptions_.remove(p);  // remove it from pending subscriptions
    }
  }
}

void NodeApp::CheckPeriodicCallbacks() {
  for (auto& cb : periodic_callbacks_) {
    cb.CallIfReady(time_now());
  }
}

void NodeApp::GetPublishers(std::vector<const publisher_base*>& pub) const {
  for (const auto& hash_and_map : publishers_) {
    for (const auto& topic_and_pub : hash_and_map.second) {
      pub.push_back(topic_and_pub.second.get());
    }
  }
}

// Sets the new state of the app.  This is used to
// allow for hooks to be run in base class on state change.
// If overriding this, call the base class's SetState method
// in the override.  This is protected since state changes
// should be triggered by events, rather than users poking
// it.
void NodeApp::SetState(NodeAppState new_state) {
  if (new_state != state_) {
    // perform entry actions for new state
    switch (new_state) {
      // Transitions to RUNNING
      case NodeApp::RUNNING:
        RunningHook();
        switch (state_) {
          case NodeApp::UNKNOWN:
          case NodeApp::INITIALIZING:
          case NodeApp::WARMUP:
            if (warmup_complete_cb_) warmup_complete_cb_();
            break;
          case NodeApp::TEARDOWN:
          case NodeApp::FAILED:
          case NodeApp::FINISHED:
            vlog_error(VCAT_GENERAL, "Can't transition to RUNNING from state %s", GetStateStr());
            break;
          case NodeApp::RUNNING:
            break;
        }
        break;

        // Transitions to TEARDOWN
      case NodeApp::TEARDOWN:
        switch (state_) {
          case NodeApp::INITIALIZING:
          case NodeApp::WARMUP:
          case NodeApp::RUNNING:
            ShutDownHook();
            break;
          default:
            break;
        }
        break;

        // Transitions to UNKNOWN - not allowed
      case NodeApp::UNKNOWN:
        vlog_fatal(VCAT_GENERAL, "Can't transition to UNKNOWN state");
        break;

        // No actions for these transitions
      case NodeApp::INITIALIZING:
      case NodeApp::WARMUP:
      case NodeApp::FAILED:
      case NodeApp::FINISHED:
        break;
    }
    state_ = new_state;
    vlog_info(VCAT_GENERAL, "%s transitioning to %s", name_.c_str(), GetStateStr());
  }
}

std::string NodeApp::PrefixTopic(const std::string& topic, bool network) {
  (void)network;
  return topic;
}
bool NodeApp::IsFieldMode() {
  std::string field_mode = "";
  std::string owner = "";
  auto res = node::get_value("field_mode", field_mode, owner);
  if (res != node::NodeError::SUCCESS) {
    vlog_debug(VCAT_GENERAL, "Failed to get mode from node registry setting mode to test");
    return false;
  }
  return field_mode == "True" ? true : false;
}
}  // namespace node
