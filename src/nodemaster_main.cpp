#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <node/core.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vlog.h>

#include <atomic>
#include <set>
#include <string>
#include <unordered_set>

#include "node/node_utils.h"
#include "node/nodemaster.h"
#include "node_registry.h"
#include "registry.h"

#define NUM_CONNECTIONS 40
#define NODEMASTER_BUFFER_SIZE 32 * 1024

#define DEBUG 0

// Glibc provides this
extern char* __progname;

static inline std::unordered_set<std::string> splitset(const std::string& s, char seperator) {
  std::unordered_set<std::string> output;
  std::string::size_type prev_pos = 0, pos = 0;

  while ((pos = s.find(seperator, pos)) != std::string::npos) {
    std::string substring(s.substr(prev_pos, pos - prev_pos));
    output.insert(substring);
    prev_pos = ++pos;
  }

  output.insert(s.substr(prev_pos, pos - prev_pos));  // Last word
  return output;
}

namespace node {

enum class ThreadAction {
  CONNECT_TO_PEERS,
  ANNOUNCE_RECONNECT,
  GET_TOPICS_FROM_PEERS,
  ANNOUNCE_SESSION_NAME,
  GET_STORE_FROM_PEERS,
  UPDATE_STORE_TO_PEERS,
};

static constexpr double PEER_SEARCH_INTERVAL_SEC = 10;
static constexpr double TOPIC_REFRESH_INTERVAL_SEC = 5;
static constexpr double KEY_VALUE_STORE_REFRESH_INTERVAL_SEC = 2;

static std::atomic<bool> nodemaster_quit = false;
static double nodemaster_start_time = -1;
static void (*quit_cb)(void) = nullptr;
static void (*shutdown_cb)(void) = nullptr;
static bool termination_requested = false;
static bool shutdown_requested = false;

void nodemaster_main_quit() { nodemaster_quit = true; }

void nodemaster_quit_callback(void (*cb)(void)) { quit_cb = cb; }
void nodemaster_shutdown_callback(void (*cb)(void)) { shutdown_cb = cb; }

struct peer_info {
  std::set<std::string> peers;                           // Total peers we know about
  std::unordered_map<std::string, double> active_peers;  // peers that are actually running

  // this will help coordinate between the network thread and the main thread
  // the network thread is safe because there is only one, the main thread only
  // does very few reads, only has to protect in case the network thread modifies
  // the classes
  std::mutex data_mtx;

  // the main thread will add some peers here, the network thread will take peers from this
  // array and merge them. The network thread is the only writer
  std::unordered_map<std::string, double> new_active_peers;

  // This queue will keep track of the work items the network thread has to process
  std::deque<ThreadAction> actions;
  // This is the mutex for the queue
  std::mutex queue_mtx;
  std::condition_variable cv;
  bool thread_should_quit = false;

  void addActionFront(ThreadAction ta) {
    std::unique_lock lk(queue_mtx);
    actions.push_front(ta);
    lk.unlock();
    cv.notify_one();
  }
};

void find_peers(peer_info& peer_info) {
  // TODO: Write code for broadcast and find other nodemasters
  (void)peer_info;
}

static void getStoreFromPeers(peer_info& peer_info, node::node_registry& reg) {
  std::set<std::string> peers_to_remove;

  for (auto& [peer, time] : peer_info.active_peers) {
    std::vector<std::string> keys, values;
    node::registry_request req = {};
    node::registry_reply reply = {};
    req.action = node::RequestType::GET_STORE;
    auto ret = send_request(peer, req, reply, 600);
    if (ret == SUCCESS) {
      if (reply.status == node::RequestStatus::SUCCESS) {
        keys = std::move(reply.store_rep[0].keys);
        values = std::move(reply.store_rep[0].values);
        // this is an error because if we hit here it mostly means someone who does not own a
        // key-value pair tried to update our store. This is not allowed
        if (!reg.update_store(keys, values, reply.store_rep[0].owner)) {
          vlog_error(VCAT_NODE, "Update our store from peer %s failed.", peer.c_str());
        }
      } else {
        // this is an error because something went wrong in the internals of the peer while trying to service
        // our normal request
        vlog_error(VCAT_NODE, "Request to get store from %s peer returned with code %d", peer.c_str(),
                   reply.status);
      }
    } else if (!shutdown_requested && (time_now() - peer_info.active_peers[peer]) > 20.0) {
      // this is a warning because sometimes we might take too long to shutdown and we might be without peers
      // for more than 20s
      vlog_warning(VCAT_NODE,
                   "Request to get store from %s peer failed. It has been more than 20s since last activity "
                   "was updated",
                   peer.c_str());
      peers_to_remove.insert(peer);
    }
  }

  std::lock_guard lk(peer_info.data_mtx);
  for (auto& p : peers_to_remove) {
    peer_info.active_peers.erase(p);
    reg.remove_topics_by_ip(p.c_str());
  }
}

static void sendUpdateToPeers(peer_info& peer_info, node::node_registry& reg) {
  std::set<std::string> peers_to_remove;
  std::vector<std::string> keys, values;
  std::vector<double> times;
  node::registry_request req = {};
  node::registry_reply reply = {};
  req.action = node::RequestType::UPDATE_STORE;
  req.update_req.resize(1);
  reg.get_current_store_snapshot(req.update_req[0].keys, req.update_req[0].values);
  req.update_req[0].owner = get_host_ips();
  for (auto& [peer, time] : peer_info.active_peers) {
    auto ret = send_request(peer, req, reply);
    if (ret == SUCCESS) {
      if (reply.status != node::RequestStatus::SUCCESS) {
        // this is an error because something went wrong in the internals of the peer while trying to service
        // our normal request
        vlog_error(VCAT_NODE, "Request to update store from %s peer returned with code %d", peer.c_str(),
                   reply.status);
      }
    } else if (!shutdown_requested && ((time_now() - peer_info.active_peers[peer]) > 20.0)) {
      // this is a warning because sometimes we might take too long to shutdown and we might be without peers
      // for more than 20s
      vlog_warning(VCAT_NODE,
                   "Request to get store from %s peer failed. It has been more than 20s since last activity "
                   "was updated",
                   peer.c_str());
      peers_to_remove.insert(peer);
    }
  }

  std::lock_guard lk(peer_info.data_mtx);
  for (auto& p : peers_to_remove) {
    peer_info.active_peers.erase(p);
    reg.remove_topics_by_ip(p.c_str());
  }
}

static void connectToPeers(peer_info& peer_info, const std::string& current_host_ip) {
  std::set<std::string> new_peers, new_active_peers;

  // search for more peers, ping all that we have and are not active
  for (auto& peer : peer_info.peers) {
    if (peer_info.active_peers.count(peer) > 0) {
      // Do not try to ping a peer we already have
      continue;
    }
    node::registry_request request;
    node::registry_reply reply;
    request.action = node::RequestType::GET_PEER_LIST;
    // The timeout for finding peers is short, we assume all peers are in the same local network
    auto res = send_request(peer, request, reply, 80);
    if (res == node::SUCCESS) {
      new_active_peers.insert(peer);
      for (auto& p : reply.peers) {
        if (!ip_in_list(p.c_str(), current_host_ip)) {
          new_peers.insert(p);
        }
      }
      for (auto& p : reply.active_peers) {
        if (peer_info.active_peers.count(p) == 0) {
          new_active_peers.insert(p);
        }
      }
    }
  }

  // get the new peers into the list
  std::lock_guard lk(peer_info.data_mtx);
  peer_info.peers.insert(new_peers.begin(), new_peers.end());
  double now = time_now();
  for (auto& p : new_active_peers) {
    peer_info.active_peers[p] = now;
  }
}

static void announceReconnect(peer_info& peer_info, node::node_registry& reg) {
  // Tell everyone we are back, if we were around, clean up
  // Also get the session name
  std::set<std::string> peers_to_remove;

  for (auto& [peer, time] : peer_info.active_peers) {
    node::registry_request request;
    node::registry_reply reply;
    request.action = node::RequestType::ANNOUNCE_RECONNECT;
    // do not care much for the answer, it should not fail
    auto res = send_request(peer, request, reply, 350);
    if (res == node::SUCCESS) {
      peer_info.active_peers[peer] = time_now();
      if (reg.is_session_name_set()) {
        if (reply.text_reply != reg.get_session_name()) {
          vlog_error(VCAT_NODE, "Error, nodemaster peer %s has session %s but we have session %s",
                     peer.c_str(), reply.text_reply.c_str(), reg.get_session_name().c_str());
        }
      } else {
        reg.set_session_name(reply.text_reply);
        vlog_info(VCAT_NODE, "Setting session name to %s, from peer %s", reply.text_reply.c_str(),
                  peer.c_str());
      }
    } else {
      vlog_warning(
          VCAT_NODE,
          "Active peer %s is not replying to announce, last message seen %.1fs ago, will move to inactive",
          peer.c_str(), time_now() - peer_info.active_peers[peer]);
      peers_to_remove.insert(peer);
    }
  }

  {
    std::lock_guard lk(peer_info.data_mtx);
    for (auto& p : peers_to_remove) peer_info.active_peers.erase(p);
  }
}

// This function will handshake will all other peers trying to get peer information from them
static void getTopicsFromPeers(peer_info& peer_info, const std::string& current_host_ip,
                               node::node_registry& reg) {
  std::set<std::string> peers_to_remove;

  for (auto& [peer, time] : peer_info.active_peers) {
    node::registry_request request;
    node::registry_reply reply;
    request.action = node::RequestType::GET_NETWORK_TOPICS;
    request.host_ip = current_host_ip;
    reg.copy_network_topic_names_to_str(request.topic_name);
    auto res = send_request(peer, request, reply, 100);
    if (res == node::SUCCESS) {
      vlog_debug(VCAT_NODE, "Got %zu topics from peer %s", reply.topics.size(), peer.c_str());
      for (auto& tp : reply.topics) {
        topic_info existing;
        if (reg.get_topic_info(tp.topic_name, existing)) {
          if (existing.cn_info.is_network && existing.cn_info.channel_ip == tp.cn_info.channel_ip &&
              existing.cn_info.channel_port == tp.cn_info.channel_port) {
            // We already have this topic, it is ok to skip anything else
            continue;
          }
        }
        if (tp.cn_info.channel_ip == current_host_ip) {
          vlog_fatal(VCAT_NODE,
                     "We received topic (%s) %s at startup with our own IP, peer %s needs to be restarted",
                     tp.topic_name.c_str(), tp.cn_info.is_network ? "NET" : "SHM", peer.c_str());
        }
        if (!reg.create_topic(tp)) {
          vlog_fatal(VCAT_NODE, "Error importing network topic: %s", tp.topic_name.c_str());
        } else {
          vlog_debug(VCAT_NODE, "Imported topic: %s on ip: %s, visible: %d", tp.topic_name.c_str(),
                     tp.cn_info.channel_ip.c_str(), tp.visible);
        }
      }
      peer_info.active_peers[peer] = time_now();
    } else if (!shutdown_requested && (time_now() - peer_info.active_peers[peer]) > 20.0) {
      vlog_warning(
          VCAT_NODE,
          "Active peer %s is not replying to topic query, last message seen %.1fs ago, will move to inactive",
          peer.c_str(), time_now() - peer_info.active_peers[peer]);
      peers_to_remove.insert(peer);
    }
  }

  std::lock_guard lk(peer_info.data_mtx);
  for (auto& p : peers_to_remove) {
    peer_info.active_peers.erase(p);
    reg.remove_topics_by_ip(p.c_str());
  }
}

void announceSessionName(peer_info& peer_info, const std::string& current_host_ip,
                         const std::string& session_name) {
  std::set<std::string> peers_to_remove;

  for (auto& [peer, time] : peer_info.active_peers) {
    node::registry_request request;
    node::registry_reply reply;
    request.action = node::RequestType::ANNOUNCE_SESSION_NAME;
    request.host_ip = current_host_ip;
    request.topic_name = session_name;
    auto res = send_request(peer, request, reply, 200);
    if (res == node::SOCKET_SERVER_NOT_FOUND) {
      vlog_warning(VCAT_NODE, "Could not announce the new session name [%s] to peer: %s, peer did not reply",
                   session_name.c_str(), peer.c_str());
      peers_to_remove.insert(peer);
    } else if (res != node::SUCCESS) {
      vlog_error(VCAT_NODE, "Could not announce the new session name [%s] to peer: %s, error: %s",
                 session_name.c_str(), peer.c_str(), NodeErrorToStr(res));
      peers_to_remove.insert(peer);
    }
  }

  std::lock_guard lk(peer_info.data_mtx);
  for (auto& p : peers_to_remove) peer_info.active_peers.erase(p);
}

static bool nodemaster_handle_request(node::registry_reply& reply, node::registry_request& request,
                                      const std::string& client_ip, node::node_registry& reg,
                                      const std::string& current_host_ip, peer_info& peer_info) {
  bool ret = true;
  // TODO: Here we would do internal work for the request, creation, query...
  switch (request.action) {
    case node::RequestType::CREATE_TOPICS: {
      reply.status = node::RequestStatus::SUCCESS;
      for (auto& tp : request.topics) {
        ret = reg.create_topic(tp);
        if (!ret) {
          vlog_error(VCAT_NODE, " Create topic (%s) failed", tp.topic_name.c_str());
          reply.topic_statuses.push_back(node::RequestStatus::GENERIC_ERROR);
        } else {
          vlog_debug(VCAT_NODE, "Nodemaster: Create topic (%s) succeeded", tp.topic_name.c_str());
          if (tp.visible) {
            ret = reg.make_topic_visible(tp);
            if (!ret) {
              tp.visible = false;
              reply.topic_statuses.push_back(node::RequestStatus::TOPIC_COULD_NOT_BE_MADE_VISIBLE);
            } else {
              reply.topic_statuses.push_back(node::RequestStatus::SUCCESS);
            }
          }
          reply.topics.push_back(tp);
        }
      }
      break;
    }
    case node::RequestType::ADVERTISE_TOPICS: {
      reply.status = node::RequestStatus::SUCCESS;
      for (auto& tp : request.topics) {
        // Maybe ensure the pid of the request matches what is stored
        ret = reg.make_topic_visible(tp);
        if (ret)
          reply.topic_statuses.push_back(node::RequestStatus::SUCCESS);
        else
          reply.topic_statuses.push_back(node::RequestStatus::TOPIC_NOT_FOUND);
      }
      break;
    }
    case node::RequestType::TOPIC_BY_NAME: {
      // search topic by name
      node::topic_info inf;
      inf.Init();
      ret = reg.get_topic_info(request.topic_name, inf);
      if (!ret) {
        vlog_fine(VCAT_NODE, "Query Topic by name (%s) failed", request.topic_name.c_str());
        reply.status = node::RequestStatus::TOPIC_NOT_FOUND;
      } else {
        vlog_fine(VCAT_NODE, "Query Topic by name succeeded");
        reply.status = node::RequestStatus::SUCCESS;
        reply.topics.push_back(inf);
      }
      break;
    }
    case node::RequestType::NUM_TOPICS: {
      reply.status = node::RequestStatus::SUCCESS;
      if (request.cli == 123) {
        reply.num_topics = reg.num_topics_cli();
      } else {
        reply.num_topics = reg.num_topics();
      }
      vlog_fine(VCAT_NODE, "Query Num Topics returned %d topics", reply.num_topics);
      break;
    }
    case node::RequestType::TOPIC_AT_INDEX: {
      node::topic_info inf;
      inf.Init();
      if (request.cli == 123) {
        ret = reg.get_topic_info_cli(request.topic_index, inf);
      } else {
        ret = reg.get_topic_info(request.topic_index, inf);
      }
      if (!ret) {
        vlog_fine(VCAT_NODE, "Query Topic by index failed");
        reply.status = node::RequestStatus::INDEX_OUT_OF_BOUNDS;
      } else {
        vlog_fine(VCAT_NODE, "Query Topic by index succeeded");
        reply.status = node::RequestStatus::SUCCESS;
        reply.topics.push_back(inf);
      }
      break;
    }
    case node::RequestType::GET_SESSION_NAME: {
      reply.status = node::RequestStatus::SUCCESS;
      reply.text_reply = reg.get_session_name();
      break;
    }
    case node::RequestType::GET_NEW_SESSION_NAME: {
      reply.status = node::RequestStatus::SUCCESS;
      reg.reset_log_path();
      reply.text_reply = reg.get_session_name();
      break;
    }
    case node::RequestType::GET_LOG_PATH: {
      reply.status = node::RequestStatus::SUCCESS;
      bool is_new = !reg.is_session_name_set();
      reply.text_reply = reg.get_log_path();
      if (is_new) {
        peer_info.addActionFront(ThreadAction::ANNOUNCE_SESSION_NAME);
      }
      break;
    }
    case node::RequestType::GET_LOG_FOLDER: {
      reply.status = node::RequestStatus::SUCCESS;
      reply.text_reply = reg.get_log_folder();
      break;
    }
    case node::RequestType::RESET_LOG_PATH: {
      reg.reset_log_path();
      reply.status = node::RequestStatus::SUCCESS;
      break;
    }
    case node::RequestType::GET_PEER_LOG_PATH: {
      reply.status = node::RequestStatus::SUCCESS;
      bool is_new = !reg.is_session_name_set();
      VLOG_ASSERT(is_new, "Calling GET_PEER_LOG_PATH when we have a session name, something went wrong");
      std::lock_guard lk(peer_info.data_mtx);
      for (auto& [peer, peer_time] : peer_info.active_peers) {
        node::registry_request peer_request;
        node::registry_reply peer_reply;
        peer_request.action = node::RequestType::GET_NEW_SESSION_NAME;
        auto res = send_request(peer, peer_request, peer_reply, 100);
        if (res == node::SUCCESS) {
          vlog_debug(VCAT_NODE, "Got session_name %s from peer %s", peer_reply.text_reply.c_str(),
                     peer.c_str());
          peer_info.active_peers[peer] = time_now();
          reg.set_session_name(peer_reply.text_reply);
          break;
        }
      }
      if (!reg.is_session_name_set()) {
        vlog_warning(VCAT_NODE,
                     "We expected to get the session name from a peer but could find no peers! You might get "
                     "generics");
      }
      reply.text_reply = reg.get_log_path();
      peer_info.addActionFront(ThreadAction::ANNOUNCE_SESSION_NAME);
      break;
    }
    case node::RequestType::ANNOUNCE_SESSION_NAME: {
      reg.reset_log_path();
      reg.set_session_name(request.topic_name);
      reply.status = node::RequestStatus::SUCCESS;
      break;
    }
    case node::RequestType::GET_TOPICS: {
      reply.status = node::RequestStatus::SUCCESS;
      reg.copy_all_topics_to(reply.topics);
      break;
    }
    case node::RequestType::GET_NETWORK_TOPICS: {
      reply.status = node::RequestStatus::SUCCESS;
      vlog_debug(VCAT_NODE,
                 "[Nodemaster] Got network request from peer %s for current topics (periodic check)",
                 request.host_ip.c_str());
      std::unordered_set<std::string> skip_set = splitset(request.topic_name, ';');
      reg.copy_network_topics_to(reply.topics, skip_set);
      // Another nodeserver might be advertising itself,
      // keep track so we can tell them of new topics
      if (!ip_in_list(client_ip.data(), current_host_ip)) {
        // the set ensures there are no duplicates
        std::lock_guard lk(peer_info.data_mtx);
        peer_info.new_active_peers[client_ip] = time_now();
      }
      break;
    }
    case node::RequestType::ANNOUNCE_NETWORK_TOPICS: {
      // take these topics and put them in reg, reply if there
      // are duplicates
      reply.status = node::RequestStatus::SUCCESS;
      for (auto& tp : request.topics) {
        topic_info existing;
        if (reg.get_topic_info(tp.topic_name, existing)) {
          if (existing.cn_info.is_network && existing.cn_info.channel_ip == tp.cn_info.channel_ip &&
              existing.cn_info.channel_port == tp.cn_info.channel_port) {
            // We already have this topic, it is ok to skip anything else
            continue;
          }
        }
        if (!reg.create_topic(tp)) {
          vlog_error(VCAT_NODE, "Error importing network topic: %s", tp.topic_name.c_str());
          reply.topics.push_back(tp);
          reply.topic_statuses.push_back(node::RequestStatus::GENERIC_ERROR);
        }
      }
      break;
    }
    case node::RequestType::GET_PEER_LIST: {
      reply.status = node::RequestStatus::SUCCESS;
      std::lock_guard lk(peer_info.data_mtx);
      for (auto& peer : peer_info.peers) {
        reply.peers.push_back(peer);
      }
      for (auto& [peer, time] : peer_info.active_peers) {
        reply.active_peers.push_back(peer);
      }

      // Another nodeserver might be advertising itself,
      // keep track so we can tell them of new topics
      if (!ip_in_list(client_ip.c_str(), current_host_ip)) {
        // the set ensures there are no duplicates
        peer_info.new_active_peers[client_ip] = time_now();
      }
      break;
    }
    case node::RequestType::ANNOUNCE_RECONNECT: {
      reply.status = node::RequestStatus::SUCCESS;
      reply.text_reply = reg.get_session_name();

      // a nodemaster is reconnecting. remove its topics from our list
      reg.remove_topics_by_ip(client_ip.c_str());
      break;
    }
    case node::RequestType::GET_NODEMASTER_INFO: {
      reply.status = node::RequestStatus::SUCCESS;
      reply.text_reply = __progname;
      reply.uptime = time_now() - nodemaster_start_time;
      reply.nodemaster_pid = (int)getppid();
      std::lock_guard lk(peer_info.data_mtx);
      for (auto& peer : peer_info.peers) {
        reply.peers.push_back(peer);
      }
      for (auto& [peer, time] : peer_info.active_peers) {
        reply.active_peers.push_back(peer);
      }
      break;
    }
    case node::RequestType::REQUEST_TERMINATION: {
      reply.status = node::RequestStatus::SUCCESS;
      vlog_info(VCAT_NODE, "Nodemaster terminating due to request from %s", request.topic_name.c_str());
      nodemaster_main_quit();
      termination_requested = true;
      break;
    }
    case node::RequestType::REQUEST_SHUTDOWN: {
      reply.status = node::RequestStatus::SUCCESS;
      if (shutdown_cb) {
        shutdown_cb();
        shutdown_requested = true;
      } else {
        vlog_error(VCAT_NODE,
                   "Recieved request to shut down but there was no shut down callback set hence terminating");
        nodemaster_main_quit();
        termination_requested = true;
      }
      break;
    }
    case node::RequestType::UPDATE_STORE: {
      if (!request.update_req.empty()) {
        const auto& req = request.update_req[0];
        bool ret = reg.update_store(req.keys, req.values, req.owner);
        vlog_debug(VCAT_NODE, "Nodemaster made update request to the store with %lu events", req.keys.size());
        if (ret)
          reply.status = node::RequestStatus::SUCCESS;
        else
          reply.status = node::RequestStatus::UNAUTHORIZED;
      }
      break;
    }
    case node::RequestType::SET_VALUE: {
      if (!request.update_req.empty()) {
        const auto& req = request.update_req[0];
        bool ret = reg.set_value(req.keys[0], req.values[0], req.owner);
        vlog_debug(VCAT_NODE, "Nodemaster made set request to the store with (%s, %s). The ret was %d",
                   req.keys[0].c_str(), req.values[0].c_str(), ret);
        if (ret)
          reply.status = node::RequestStatus::SUCCESS;
        else
          reply.status = node::RequestStatus::UNAUTHORIZED;
      } else {
        // could not service the request
        reply.status = node::RequestStatus::REQUEST_INVALID;
      }
      break;
    }
    case node::RequestType::UPDATE_PEERS: {
      peer_info.addActionFront(ThreadAction::UPDATE_STORE_TO_PEERS);
      break;
    }
    case node::RequestType::GET_VALUE: {
      if (!request.update_req.empty()) {
        const auto& key = request.update_req[0].keys[0];
        reply.store_rep = {{.keys = {key}, .values = {reg.get_value(key)}, .owner = reg.get_owner(key)}};
        const auto& value = reply.store_rep[0].values[0];
        if (value.empty()) {
          reply.status = node::RequestStatus::KEY_NOT_FOUND;
        } else {
          reply.status = node::RequestStatus::SUCCESS;
        }
      } else {
        // could not service the request
        reply.status = node::RequestStatus::REQUEST_INVALID;
      }
      break;
    }
    case node::RequestType::GET_STORE: {
      reply.store_rep.resize(1);
      store& snapshot = reply.store_rep[0];
      reg.get_current_store_snapshot(snapshot.keys, snapshot.values, request.all, request.search_term);
      snapshot.owner = current_host_ip;
      reply.status = node::RequestStatus::SUCCESS;
      break;
    }
    default:
      reply.status = node::RequestStatus::REQUEST_INVALID;
  }
  return ret;
}

void thread_function(peer_info& peer_info, node::node_registry& reg, const std::string& current_host_ip) {
  double last_peer_check = time_now();
  double last_topic_refresh = time_now();
  double last_store_update = time_now();
  using namespace std::chrono_literals;
  while (!peer_info.thread_should_quit) {
    // Merge new_active_peers into active peers
    std::unique_lock dtlk(peer_info.data_mtx);
    if (!peer_info.new_active_peers.empty()) {
      for (auto& [peer, time] : peer_info.new_active_peers) {
        peer_info.active_peers[peer] = time;
      }
      peer_info.new_active_peers.clear();
    }
    dtlk.unlock();

    std::unique_lock lk(peer_info.queue_mtx);

    if (peer_info.actions.empty()) {
      // if the queue is empty, we will wait for work on it
      if (peer_info.cv.wait_for(lk, 200ms,
                                [&] { return !peer_info.actions.empty() || peer_info.thread_should_quit; })) {
        // We may have an action in the queue
        if (peer_info.actions.empty()) {
          continue;
        }
      } else {
        lk.unlock();
        // This is the timeout case, we can to timed operations here
        double now = time_now();
        if (now - last_peer_check > PEER_SEARCH_INTERVAL_SEC) {
          connectToPeers(peer_info, current_host_ip);
          last_peer_check = now;
        } else if (now - last_topic_refresh > TOPIC_REFRESH_INTERVAL_SEC) {
          getTopicsFromPeers(peer_info, current_host_ip, reg);
          last_topic_refresh = now;
        }
        if (now - last_store_update > KEY_VALUE_STORE_REFRESH_INTERVAL_SEC) {
          getStoreFromPeers(peer_info, reg);
          last_store_update = now;
        }
        continue;
      }
    }

    VLOG_ASSERT(lk.owns_lock());
    VLOG_ASSERT(!peer_info.actions.empty());
    ThreadAction act = peer_info.actions.front();
    peer_info.actions.pop_front();
    lk.unlock();

    switch (act) {
      case ThreadAction::CONNECT_TO_PEERS: {
        connectToPeers(peer_info, current_host_ip);
        last_peer_check = time_now();
        break;
      }
      case ThreadAction::ANNOUNCE_RECONNECT: {
        announceReconnect(peer_info, reg);
        break;
      }
      case ThreadAction::GET_TOPICS_FROM_PEERS: {
        getTopicsFromPeers(peer_info, current_host_ip, reg);
        last_topic_refresh = time_now();
        break;
      }
      case ThreadAction::ANNOUNCE_SESSION_NAME: {
        announceSessionName(peer_info, current_host_ip, reg.get_session_name());
        break;
      }
      case ThreadAction::GET_STORE_FROM_PEERS: {
        getStoreFromPeers(peer_info, reg);
        break;
      }
      case ThreadAction::UPDATE_STORE_TO_PEERS: {
        sendUpdateToPeers(peer_info, reg);
        break;
      }
    }
  }
}

int nodemaster_main(const char* data_path, const char* peer_list, uint16_t node_reg_port) {
  int server_fd = -1;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);
  std::vector<char> buffer(NODEMASTER_BUFFER_SIZE);
  peer_info peer_info;
  std::string current_host_ip, client_ip;
  char client_ip_buffer[INET6_ADDRSTRLEN];
  char network_thread_name[16] = "nodem_thread";
  char nm_thread_name[16] = "nodemaster";

  pthread_setname_np(pthread_self(), nm_thread_name);
  nodemaster_start_time = time_now();

  vlog_info(VCAT_NODE, "nodemaster_main  data_path=%s, peers=%s, port=%hu", data_path, peer_list,
            node_reg_port);

  current_host_ip = get_host_ips();

  if (peer_list != nullptr) {
    if (!strcasecmp(peer_list, "all")) {
      find_peers(peer_info);
    } else {
      auto peer_vec = node::split(peer_list, ';');
      auto host_vec = node::split_set(current_host_ip, ';');
      for (auto& p : peer_vec) {
        if (!p.empty() && !host_vec.contains(p)) {
          peer_info.peers.insert(p);
        }
      }
    }
  }

  node::node_registry reg;
  reg.set_registry_ip(current_host_ip);
  vlog_debug(VCAT_NODE, "Starting server");

  if (data_path) reg.set_log_folder(data_path);

  // This first call is done inline, to get a first set of active peers
  connectToPeers(peer_info, current_host_ip);
  announceReconnect(peer_info, reg);
  peer_info.actions.push_back(ThreadAction::GET_TOPICS_FROM_PEERS);
  peer_info.actions.push_back(ThreadAction::GET_STORE_FROM_PEERS);

  std::thread network_thread(
      [&peer_info, &reg, &current_host_ip]() { thread_function(peer_info, reg, current_host_ip); });
  pthread_setname_np(network_thread.native_handle(), network_thread_name);

  // Creating socket file descriptor, with non block and cloexec
  if ((server_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0)) < 0) {
    vlog_error(VCAT_NODE, "Socket creation for nodemaster socket failed with errno %d: %s", errno,
               strerror(errno));
    exit(EXIT_FAILURE);
  }

  // Forcefully attaching socket to the port
  // Do not specify REUSEPORT, this can cause problems when there are multiple
  // nodemasters
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,  //| SO_REUSEPORT,
                 &opt, sizeof(opt))) {
    perror("setsockopt");
    exit(EXIT_FAILURE);
  }
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(node_reg_port);

  // Forcefully attaching socket to the port 8080
  if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
    perror("bind failed");
    vlog_fatal(VCAT_NODE,
               "Error bind() failed.  Nodemaster failed to start, due to an already present nodemaster. Use "
               "[sudo lsof -i :25678] to find the process that has the port open");
    exit(EXIT_FAILURE);
  }

  if (listen(server_fd, NUM_CONNECTIONS) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }

  vlog_debug(VCAT_NODE, "Nodemaster: Now starting to listen on the socket...");

  while (!nodemaster_quit) {
    ssize_t rec_size = 0;
    int new_socket;

    // Do not let accept() block execution indefinitely.
    // Otherwise the process or thread will hang and never exit.
    {
      pollfd pfd;
      pfd.fd = server_fd;
      pfd.events = POLLIN | POLLERR;
      pfd.revents = 0;
      int timeout_milliseconds = 250;
      int pollresult = poll(&pfd, 1, timeout_milliseconds);
      if (pollresult == -1) {
        perror("poll()");
        continue;
      } else if (pollresult == 0) {
        // timeout
        continue;
      }
      new_socket = accept4(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen, SOCK_CLOEXEC);
      if (new_socket < 0) {
        continue;
      }
    }

    memset(buffer.data(), 0, buffer.size());
    memset(client_ip_buffer, 0, sizeof(client_ip_buffer));
    strncpy(client_ip_buffer, inet_ntoa(address.sin_addr), sizeof(client_ip_buffer));
    client_ip = client_ip_buffer;

    // inform user of socket number - used in send and receive commands
    vlog_fine(VCAT_NODE, "New connection, socket fd %d, ip %s, port %d", new_socket,
              inet_ntoa(address.sin_addr), ntohs(address.sin_port));

    // Handle here the request
    node::registry_request request;
    node::registry_reply reply;
    request.Init();
    reply.Init();

    double read_start, handle_start, reply_start, reply_end;
    read_start = time_now();
    bool ret = read_cbuf(new_socket, buffer, rec_size, 500);
    if (!ret) {
      vlog_error(VCAT_NODE, "Nodemaster: error receiving the request");
      close(new_socket);
      continue;
    }

    ret = request.decode(buffer.data(), rec_size);
    if (!ret) {
      vlog_error(VCAT_NODE, "Nodemaster: error decoding the request");
      close(new_socket);
      continue;
    }

    printRequest(request, VL_FINE);

    handle_start = time_now();
    ret = nodemaster_handle_request(reply, request, client_ip, reg, current_host_ip, peer_info);

    size_t reply_size = reply.encode_size();
    if (reply_size > buffer.size()) {
      buffer.resize(reply_size);
    }
    ret = reply.encode(buffer.data(), buffer.size());
    if (!ret) {
      vlog_error(VCAT_NODE, "Error encoding the reply");
      printRequest(request, VL_ERROR);
      close(new_socket);
      continue;
    }

    printReply(reply, VL_FINE);

    reply_start = time_now();
    ssize_t send_bytes = send(new_socket, buffer.data(), reply_size, 0);
    if (send_bytes != reply_size) {
      vlog_error(VCAT_NODE, "Could not send the reply for this request");
      printRequest(request, VL_ERROR);
      printReply(reply, VL_ERROR);
    }

    reply_end = time_now();
    vlog_debug(VCAT_NODE, "Handled request %s, timing: Receive %fms, Handle: %fms, Reply: %fms",
               NodeRequestToStr(request.action), (handle_start - read_start) * 1000.0,
               (reply_start - handle_start) * 1000.0, (reply_end - reply_start) * 1000.0);

    close(new_socket);
  }

  if (server_fd != -1) {
    close(server_fd);
  }

  if (!peer_info.thread_should_quit) {
    peer_info.thread_should_quit = true;
    peer_info.cv.notify_one();
  }
  if (network_thread.joinable()) network_thread.join();

  if (quit_cb) {
    quit_cb();
  } else if (termination_requested) {
    // If we do not have a callback, but termination was requested,
    // ensure that we do terminate
    exit(0);
  }
  return 0;
}

}  // namespace node
