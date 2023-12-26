#include "node/nodelib.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <libexplain/libexplain.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/file.h>
#include <sys/inotify.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>

#include "circular_buffer.h"
#include "crc.h"
#include "defer.h"
#include "node/core.h"
#include "node/node_utils.h"
#include "node/transport.h"
#include "process.h"
#include "registry.h"
#include "vlog.h"

/*
    In order for this to work, it is preferable if we disable ASLR
    This way, the address from mmap will be consistent across processes

    https://askubuntu.com/questions/318315/how-can-i-temporarily-disable-aslr-address-space-layout-randomization

    sudo sysctl kernel.randomize_va_space=0
*/

#define KB (1024)
#define MB (KB * KB)

class node_access {
public:
  int lock_fd;

  node_access() { lock_fd = -1; }

  bool access(const char* lock_name, bool exclusive) {
    lock_fd = open(lock_name, O_RDWR | O_CREAT, 0744);
    if (lock_fd == -1) {
      perror("Could not open the lock file: ");
      return false;
    }

    int mode = (exclusive ? LOCK_EX : LOCK_SH);

    int lock_return = flock(lock_fd, mode);
    if (lock_return == -1) {
      perror("Could not get the lock for node: ");
      return false;
    }
    return true;
  }

  ~node_access() {
    if (lock_fd != -1) {
      flock(lock_fd, LOCK_UN);
      close(lock_fd);
      lock_fd = -1;
    }
  }
};

namespace node {

NodeError check_master(const std::string& host) {
  /*
   node_access node_lock;

   int ret = node_lock.access("/tmp/node.lock", true);
   if (!ret) {
       return false;
   }

   mem_fd = shm_open("/verdant_node", O_RDWR | O_CREAT, S_IRWXU);

   // Ensure we have enough space
   ftruncate(mem_fd, mem_length);

   addr = mmap(NULL, mem_length, PROT_READ | PROT_WRITE,
                       MAP_SHARED, mem_fd, 0);

   if (addr == (void *)-1) {
       printf("Got error as: %d\n", errno);
       addr = nullptr;
       return false;
   }
*/
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::NUM_TOPICS;
  return send_request(hostname, req, reply);
}

// Get the number of open channels on the system
NodeError num_channels(u32& outval, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::NUM_TOPICS;
  auto ret = send_request(hostname, req, reply);
  if (ret == SUCCESS) {
    outval = reply.num_topics;
  }
  return ret;
}

static NodeError RequestStatusToNodeError(const node::RequestStatus st) {
  switch (st) {
    case node::RequestStatus::SUCCESS:
      return SUCCESS;
    case node::RequestStatus::TOPIC_NOT_FOUND:
      return TOPIC_NOT_FOUND;
    case node::RequestStatus::INDEX_OUT_OF_BOUNDS:
      return INDEX_OUT_OF_BOUNDS;
    case node::RequestStatus::REQUEST_INVALID:
      return SERVER_INCOMPATIBLE;
    case node::RequestStatus::KEY_NOT_FOUND:
      return KEY_NOT_FOUND;
    case node::RequestStatus::TOPIC_COULD_NOT_BE_MADE_VISIBLE:
      return TOPIC_COULD_NOT_BE_MADE_VISIBLE;
    case node::RequestStatus::UNAUTHORIZED:
      return KEY_UNAUTHORIZED;
    case node::RequestStatus::GENERIC_ERROR:
    default:
      return GENERIC_ERROR;
  }
}

// This function retrieves the channel info based on the index, the
// info parameter is output. The function returns false if there is no
// channel on that index
NodeError get_topic_info(u32 channel_index, topic_info& info, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::TOPIC_AT_INDEX;
  req.topic_index = channel_index;
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;

  if (reply.status != node::RequestStatus::SUCCESS) {
    return RequestStatusToNodeError(reply.status);
  }

  info = reply.topics[0];
  return SUCCESS;
}

// Get all the topics
NodeError get_all_topic_info(std::vector<topic_info>& vec, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::GET_TOPICS;
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;

  if (reply.status != node::RequestStatus::SUCCESS) {
    return RequestStatusToNodeError(reply.status);
  }

  vec = std::move(reply.topics);
  return SUCCESS;
}

NodeError make_topic_visible(const std::string& name, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::ADVERTISE_TOPICS;
  topic_info inf;
  inf.topic_name = name;
  req.topics.push_back(inf);
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;

  if (reply.status != node::RequestStatus::SUCCESS) {
    return RequestStatusToNodeError(reply.status);
  }

  return SUCCESS;
}

// Create a new channel on the system, with the information on info
NodeError create_topic(const topic_info& info, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::CREATE_TOPICS;
  req.topics.push_back(info);
  req.topics[0].publisher_pid = get_my_pid();

  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) {
    vlog_error(VCAT_NODE, "Error creating topic %s : %s", info.topic_name.c_str(), NodeErrorToStr(ret));
    return ret;
  }
  if (reply.status == node::RequestStatus::SUCCESS) {
    //        printf("Create topic succeeded\n");
    return SUCCESS;
  } else {
    //        printf("Create topic failed\n");
    return RequestStatusToNodeError(reply.status);
  }
}

// Get information on a topic on the system, based on the name of the topic.
// returns false if there is no topic with that name
NodeError get_topic_info(const std::string& name, topic_info& info, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::TOPIC_BY_NAME;
  req.topic_name = name;

  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;

  if (reply.status != node::RequestStatus::SUCCESS) {
    return RequestStatusToNodeError(reply.status);
  }

  info = reply.topics[0];
  return SUCCESS;
}

// Get the name of the current session
std::string get_session_name(const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::GET_SESSION_NAME;

  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return std::string();

  if (reply.status != node::RequestStatus::SUCCESS) {
    return std::string();
  }

  return reply.text_reply;
}

std::string get_log_path(const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::GET_LOG_PATH;

  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return std::string();

  if (reply.status != node::RequestStatus::SUCCESS) {
    return std::string();
  }

  return reply.text_reply;
}

NodeError set_value(const std::string& key, const std::string& value, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);
  req.action = node::RequestType::SET_VALUE;
  std::string host_ip = get_host_ips();
  req.update_req = {{.keys = {key}, .values = {value}, .owner = host_ip}};
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;
  if (reply.status != node::RequestStatus::SUCCESS) return RequestStatusToNodeError(reply.status);
  return NodeError::SUCCESS;
}

NodeError get_value(const std::string& key, std::string& value, std::string& owner, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);
  req.action = node::RequestType::GET_VALUE;
  req.update_req = {{.keys = {key}, .values = {""}, .owner = ""}};
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;
  if (reply.status != node::RequestStatus::SUCCESS) return RequestStatusToNodeError(reply.status);
  // for sanity
  if (reply.store_rep.empty() || reply.store_rep[0].values.empty()) return GENERIC_ERROR;
  value = reply.store_rep[0].values[0];
  owner = reply.store_rep[0].owner;
  return NodeError::SUCCESS;
}

NodeError get_store(std::vector<std::string>& keys, std::vector<std::string>& values, const std::string& host,
                    bool all, const std::string& search_regex_term) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);
  req.action = node::RequestType::GET_STORE;
  req.all = all;
  req.search_term = search_regex_term;
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;
  if (reply.status != node::RequestStatus::SUCCESS) return RequestStatusToNodeError(reply.status);

  // for sanity
  if (reply.store_rep.empty() || reply.store_rep[0].values.empty()) return GENERIC_ERROR;

  keys = std::move(reply.store_rep[0].keys);
  values = std::move(reply.store_rep[0].values);
  return NodeError::SUCCESS;
}

// Make a request to nodemaster running at host to send updates to all its active peers
NodeError send_update_store_to_peers(const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);
  req.action = node::RequestType::UPDATE_PEERS;
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;
  if (reply.status != node::RequestStatus::SUCCESS) return RequestStatusToNodeError(reply.status);

  return NodeError::SUCCESS;
}

std::string get_log_folder(const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::GET_LOG_FOLDER;

  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return std::string();

  if (reply.status != node::RequestStatus::SUCCESS) {
    return std::string();
  }

  return reply.text_reply;
}

std::string get_peer_log_path(const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::GET_PEER_LOG_PATH;

  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return std::string();

  if (reply.status != node::RequestStatus::SUCCESS) {
    return std::string();
  }

  return reply.text_reply;
}

NodeError reset_log_path(const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::RESET_LOG_PATH;

  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;

  if (reply.status != node::RequestStatus::SUCCESS) {
    return RequestStatusToNodeError(reply.status);
  }

  return SUCCESS;
}

NodeError get_nodemaster_info(double& uptime, int& nm_pid, std::string& process_name,
                              std::vector<std::string>& active_peers) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = "127.0.0.1";

  req.action = node::RequestType::GET_NODEMASTER_INFO;

  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;

  if (reply.status != node::RequestStatus::SUCCESS) {
    return RequestStatusToNodeError(reply.status);
  }

  uptime = reply.uptime;
  process_name = reply.text_reply;
  active_peers = reply.active_peers;
  nm_pid = reply.nodemaster_pid;

  return SUCCESS;
}

NodeError request_nodemaster_termination(const std::string& who, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::REQUEST_TERMINATION;
  req.topic_name = who;

  vlog_debug(VCAT_NODE, "The termination request is being sent to %s", hostname.c_str());
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;

  if (reply.status != node::RequestStatus::SUCCESS) {
    return RequestStatusToNodeError(reply.status);
  }

  return SUCCESS;
}

NodeError request_nodemaster_shutdown(const std::string& who, const std::string& host) {
  node::registry_request req = {};
  node::registry_reply reply = {};
  std::string hostname = (host.empty() ? "127.0.0.1" : host);

  req.action = node::RequestType::REQUEST_SHUTDOWN;
  req.topic_name = who;

  vlog_debug(VCAT_NODE, "The shutdown request is being sent to %s", hostname.c_str());
  auto ret = send_request(hostname, req, reply);
  if (ret != SUCCESS) return ret;

  if (reply.status != node::RequestStatus::SUCCESS) {
    return RequestStatusToNodeError(reply.status);
  }

  return SUCCESS;
}

void MsgBufPtr::release() {
  if (ptr_ != nullptr) {
    assert(obs_ != nullptr);
    obs_->release_message(*this);
  }
  ptr_ = nullptr;
  size_ = 0;
  obs_ = nullptr;
}

NotificationManager::~NotificationManager() {
  if (inotify_fd_ != -1) {
    ::close(inotify_fd_);
    inotify_fd_ = -1;
  }
  notification_cbs_.clear();
}

bool NotificationManager::setup_epoll() {
  if (epoll_fd_ > -1) return true;
  epoll_fd_ = epoll_create1(0);
  if (epoll_fd_ == -1) {
    vlog_error(VCAT_NODE, "Error creating an epoll instance: %d - %s", errno, strerror(errno));
    return false;
  }
  return true;
}

bool NotificationManager::setup_inotify() {
  if (inotify_fd_ > -1) return true;
  // open inotify here, non blocking for epoll
  inotify_fd_ = inotify_init1(IN_NONBLOCK);
  if (inotify_fd_ == -1) {
    vlog_error(VCAT_NODE, "Problem creating an inotify instance: %d - %s", errno, strerror(errno));
    return false;
  }
  VLOG_ASSERT(epoll_fd_ > -1);

  struct epoll_event ev;
  // Should we use Event Trigger for epoll?
  ev.events = EPOLLIN;
  ev.data.fd = inotify_fd_;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, inotify_fd_, &ev) == -1) {
    vlog_error(VCAT_NODE, "Problem adding the inotify instance to epoll: %d - %s", errno, strerror(errno));
    return false;
  }
  return true;
}
int32_t NotificationManager::add_shm_notification(const std::string& fname, NotifyCallback cb,
                                                  const std::string& topic_name) {
  if (!setup_epoll()) return -1;
  if (!setup_inotify()) return -1;

  // add inotify
  int notify_file_wd = inotify_add_watch(inotify_fd_, fname.c_str(), IN_MODIFY);
  if (notify_file_wd < 0) {
    vlog_error(VCAT_NODE, "Problem creating an inotify watch for %s : %d - %s", fname.c_str(), errno,
               strerror(errno));
    return -1;
  }

  const uint64_t key = ((uint64_t)notify_file_wd << 32) | inotify_fd_;
  VLOG_ASSERT(notification_cbs_.count(key) == 0, "We had a duplicate entry for %d %d, inotify file %s",
              inotify_fd_, notify_file_wd, fname.c_str());
  notification_cbs_[key] = cb;
  fd_to_names_[key] = topic_name;
  return notify_file_wd;
}

bool NotificationManager::add_socket_notification(int sockfd, NotifyCallback cb,
                                                  const std::string& topic_name) {
  if (!setup_epoll()) return false;

  VLOG_ASSERT(notification_cbs_.count(sockfd) == 0, "We had a duplicate inotify socket entry for %d", sockfd);

  struct epoll_event ev;
  // Should we use Event Trigger for epoll?
  ev.events = EPOLLIN;
  ev.data.fd = sockfd;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, sockfd, &ev) == -1) {
    vlog_error(VCAT_NODE, "Problem adding socket %d instance to epoll: %d - %s", sockfd, errno,
               strerror(errno));
    return false;
  }

  notification_cbs_[sockfd] = cb;
  fd_to_names_[sockfd] = topic_name;
  return true;
}

void NotificationManager::remove_notification(int32_t fd, int32_t wd) {
  const uint64_t key = ((uint64_t)wd << 32) | fd;

  notification_cbs_.erase(key);

  fd_to_names_.erase(key);
}

void NotificationManager::remove_shm_notification(int32_t wd) {
  if (inotify_rm_watch(inotify_fd_, wd) == -1) {
    vlog_warning(VCAT_NODE, "Problem removing inotifyz %d: %d - %s", wd, errno, strerror(errno));
  }
  remove_notification(inotify_fd_, wd);
}

void NotificationManager::remove_socket_notification(int32_t fd) {
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) == -1) {
    vlog_warning(VCAT_NODE, "Problem removing socket %d instance to epoll: %d - %s", fd, errno,
                 strerror(errno));
  }
  remove_notification(fd, 0);
}

static std::string NotifyEventToStr(const struct inotify_event* event) {
  std::string s;
  s.reserve(100);
  char buf[32];
  snprintf(buf, sizeof(buf), "%X", event->mask);

  s = "Inotify Event mask ";
  s += buf;

  if (event->mask & IN_ACCESS) {
    s += " IN_ACCESS";
  }
  if (event->mask & IN_MODIFY) {
    s += " IN_MODIFY";
  }
  if (event->mask & IN_ATTRIB) {
    s += " IN_ATTRIB";
  }
  if (event->mask & IN_CLOSE_WRITE) {
    s += " IN_CLOSE_WRITE";
  }
  if (event->mask & IN_CLOSE_NOWRITE) {
    s += " IN_CLOSE_NOWRITE";
  }
  if (event->mask & IN_OPEN) {
    s += " IN_OPEN";
  }
  if (event->mask & IN_MOVED_FROM) {
    s += " IN_MOVED_FROM";
  }
  if (event->mask & IN_MOVED_TO) {
    s += " IN_MOVED_TO";
  }
  if (event->mask & IN_CREATE) {
    s += " IN_CREATE";
  }
  if (event->mask & IN_DELETE) {
    s += " IN_DELETE";
  }
  if (event->mask & IN_DELETE_SELF) {
    s += " IN_DELETE_SELF";
  }
  if (event->mask & IN_MOVE_SELF) {
    s += " IN_MOVE_SELF";
  }
  if (event->mask & IN_UNMOUNT) {
    s += " IN_UNMOUNT";
  }
  if (event->mask & IN_Q_OVERFLOW) {
    s += " IN_Q_OVERFLOW";
  }
  if (event->mask & IN_IGNORED) {
    s += " IN_IGNORED";
  }
  return s;
}

void NotificationManager::handle_inotify_events() {
  int len = 0;
  const struct inotify_event* event = nullptr;

  len = read(inotify_fd_, buf_, sizeof(buf_));
  if (len == -1) {
    if (errno == EAGAIN || errno == EINTR) {
      // Nothing to do, we will try more later
      return;
    }
    vlog_error(VCAT_NODE, "Error on reading from inotify FD: %d - %s", errno, strerror(errno));
    return;
  }

  // Loop over the events from inotify
  for (char* ptr = buf_; ptr < buf_ + len; ptr += sizeof(struct inotify_event) + event->len) {
    event = (const struct inotify_event*)ptr;
    if (event->wd < 0) {
      std::string s = NotifyEventToStr(event);
      vlog_warning(VCAT_NODE, "Event problem: %s", s.c_str());
      continue;
    }

    const uint64_t key = ((uint64_t)event->wd << 32) | inotify_fd_;

    if (notification_cbs_.count(key) == 0) {
      // There's a race condition between removing a WD and draining the queue of events
      // it's fairly harmless, WDs aren't reused until we hit INT_MAX, so getting a stray notification
      // won't do something bad like calling the wrong callback
      // ...but if you see this repeatedly logged, we have a bug
      vlog_error(VCAT_GENERAL, "We should have seen inotify WD %d before (benign race condition?)",
                 event->wd);
    } else {
      // execute the callback
      notification_cbs_[key](event->wd);
    }
  }
}

int NotificationManager::wait_for_notification(int timeout_ms) {
  struct epoll_event events[128];
  int nfds;

  if (epoll_fd_ == -1) {
    // Nothing is queued, same as a timeout
    return 0;
  }
  nfds = epoll_wait(epoll_fd_, events, 128, timeout_ms);
  if (nfds == 0) {
    // timeout
    return 0;
  }
  if (nfds < 0) {
    if (errno == EINTR) {
      // Interrupted by a signal, or timeout
      return 0;
    }
    vlog_error(VCAT_NODE, "Error on epoll_wait: %d - %s", errno, strerror(errno));
    return -1;
  }

  for (int s = 0; s < nfds; s++) {
    int filedes = events[s].data.fd;
    if (filedes == inotify_fd_) {
      // Handle inotify reading events
      handle_inotify_events();
    } else {
      if (events[s].events & EPOLLIN) {
        VLOG_ASSERT(notification_cbs_.count(filedes) > 0,
                    "FD %d shoulde be a socket we have registered on inotify", filedes);
        // Call the callback
        notification_cbs_[filedes](filedes);
      } else if (events[s].events & (EPOLLERR | EPOLLHUP)) {
        remove_socket_notification(filedes);
      }
    }
  }
  return nfds;
}

/// This function is meant to create the shared memory for a shm_channel
//  It returns a pointer to the mapped memory for sharing
void* helper_open_channel(const channel_info& info, int& mem_fd) {
  mem_fd = shm_open(info.channel_path.c_str(), O_RDWR | O_CREAT, S_IRWXU);
  if (mem_fd < 0) {
    vlog_error(VCAT_NODE, "Failed to create shared memory %s: %s", info.channel_path.c_str(),
               strerror(errno));
    return nullptr;
  }

  // Ensure we have enough space
  auto _ = ftruncate(mem_fd, info.channel_size);
  (void)_;

  void* addr = mmap(nullptr, info.channel_size, PROT_READ | PROT_WRITE, MAP_SHARED, mem_fd, 0);

  if (addr == (void*)-1) {
    vlog_error(VCAT_NODE, "Failed to create shared memory %s: %s", info.channel_path.c_str(),
               explain_mmap(nullptr, info.channel_size, PROT_READ | PROT_WRITE, MAP_SHARED, mem_fd, 0));
    addr = nullptr;
    close(mem_fd);
  }

  return addr;
}

void helper_clean(void* addr, int mem_fd, u32 mem_length) {
  if (addr != nullptr) {
    munmap(addr, mem_length);
  }
  if (mem_fd > -1) {
    close(mem_fd);
  }
}

publisher_impl_shm::publisher_impl_shm(u32 num_el, u32 elem_size) {
  aligned_elem_size_ = (elem_size + 15) & ~0x0F;
  num_elems_ = num_el;
}

void publisher_impl_shm::fill_channel_info(topic_info& info) {
  u32 sz = (sizeof(circular_buffer)                 // One circular buffer
            + num_elems_ * sizeof(message_bookkeep)  // bookkeep data
            + num_elems_ * aligned_elem_size_);       // the actual elements
  sz += 32;
  u32 remainder = sz % 1024;
  if (remainder != 0) {
    sz = sz + 1024 - remainder;
  }

  info.cn_info.channel_path = "/node_";
  auto tmp_string = info.topic_name;
  for (char& c : tmp_string)
    if (c == '/') c = '_';
  info.cn_info.channel_path += std::to_string(info.msg_hash) + "_" + tmp_string;
  info.cn_info.channel_size = sz;
  info.cn_info.channel_notify = "/tmp" + info.cn_info.channel_path + ".sock";
}

NodeError publisher_impl_shm::open(const topic_info* info) {
  VLOG_ASSERT(aligned_elem_size_ > 0, "Aligned elem size can never be zero");

  data_ = (u8*)helper_open_channel(info->cn_info, mem_fd_);
  if (!data_) {
    vlog_error(VCAT_NODE, "Could not open the shared memory");
    return SHARED_MEMORY_OPEN_ERROR;
  }

  mem_length_ = info->cn_info.channel_size;

  notify_fd_ = ::open(info->cn_info.channel_notify.c_str(), O_CREAT | O_RDWR, 0644);
  if (notify_fd_ == -1) {
    vlog_error(VCAT_NODE, "Could not open the file %s for notification: %d - %s",
               info->cn_info.channel_notify.c_str(), errno, strerror(errno));
    return NOTIFY_FILE_OPEN_ERROR;
  }

  // do setup of stuff in data now!
  indices_ = (circular_buffer*)data_;
  bk_ = (message_bookkeep*)(data_ + sizeof(circular_buffer));
  elems_ = ((u8*)data_ + sizeof(circular_buffer) + num_elems_ * sizeof(message_bookkeep));
  elems_ += 15;
  elems_ = (u8*)(((uintptr_t)elems_) & ~0x0F);
  indices_->initialize(num_elems_, aligned_elem_size_, bk_);
  VLOG_ASSERT(aligned_elem_size_ == indices_->get_aligned_elem_size(), "Ensure we have the size we indicated");
  return SUCCESS;
}

u8* publisher_impl_shm::get_memory_for_message() {
  // This call might block
  unsigned int elem_index = indices_->get_next_empty(bk_);
  // See http://lists.llvm.org/pipermail/cfe-dev/2012-July/023422.html
  // Do not put the () here!!
  return &elems_[elem_index * aligned_elem_size_];
}

bool publisher_impl_shm::transmit_message(const u8*, size_t) {
  indices_->publish(bk_);
  if (notify_fd_ != -1) {
    char c = 1;
    ::lseek(notify_fd_, SEEK_SET, 0);
    const auto written_n = ::write(notify_fd_, &c, 1);
    assert(written_n == 1);
    (void)written_n;
  }
  return true;
}

void publisher_impl_shm::close() {
  helper_clean(data_, mem_fd_, mem_length_);
  if (notify_fd_ != -1) {
    ::close(notify_fd_);
    notify_fd_ = -1;
  }
  data_ = nullptr;
  mem_length_ = 0;
  mem_fd_ = -1;
  elems_ = nullptr;
  indices_ = nullptr;
  bk_ = nullptr;
}

u64 publisher_impl_shm::get_num_published() const { return indices_->get_num_packets(); }

publisher_impl_shm::~publisher_impl_shm() { close(); }

NodeError subscribe_impl_shm::open(const channel_info& cn_info, u32 num, bool quiet, float timeout_sec) {
  // Now we have the topic info on info
  data_ = (u8*)helper_open_channel(cn_info, mem_fd_);
  if (!data_) {
    return SHARED_MEMORY_OPEN_ERROR;
  }

  mem_length_ = cn_info.channel_size;
  num_elems_ = num;

  // do setup of stuff in data now!
  indices_ = (circular_buffer*)data_;
  bk_ = (message_bookkeep*)(data_ + sizeof(circular_buffer));
  elems_ = (((u8*)bk_) + indices_->get_buf_size() * sizeof(message_bookkeep));
  elems_ += 15;
  elems_ = (u8*)(((uintptr_t)elems_) & ~0x0F);

  if (aligned_elem_size_ == 0) {
    aligned_elem_size_ = indices_->get_aligned_elem_size();
  } else if (aligned_elem_size_ != indices_->get_aligned_elem_size()) {
    vlog_error(
        VCAT_NODE,
        "Producer and subscriber disagree on message size for topic %s (subscriber %d vs publisher %d)",
        topic_name_.c_str(), aligned_elem_size_, indices_->get_aligned_elem_size());
    u32* pu = (u32*)data_;
    vlog_error(VCAT_NODE, "Dumping circular buffer: %X %X %X %X %X", pu[0], pu[1], pu[2], pu[3], pu[4]);
    return NodeError::PRODUCER_CBUF_SIZE_MISMATCH;
  }

  last_index_ = indices_->get_starting_last_index(bk_);

  return node::SUCCESS;
}

NodeError subscribe_impl_shm::open_notify(const channel_info& cn_info, u32 num_elems, NotificationManager& nm,
                                          NotificationManager::NotifyCallback cb, bool quiet,
                                          float timeout_sec) {
  // We don't use this in shm
  (void)timeout_sec;
  NodeError res;
  res = open(cn_info, num_elems);
  if (res != SUCCESS) return res;
  wd_ = nm.add_shm_notification(cn_info.channel_notify, cb, topic_name_);
  if (wd_ == -1) {
    if (!quiet) {
      vlog_error(VCAT_NODE, "Issue adding Notification callback for topic %s", topic_name_.c_str());
    }
    return NodeError::GENERIC_ERROR;
  }
  notification_manager_ = &nm;
  return node::SUCCESS;
}

void subscribe_impl_shm::close() {
  if (notification_manager_) {
    notification_manager_->remove_shm_notification(wd_);
    notification_manager_ = nullptr;
  }
  helper_clean(data_, mem_fd_, mem_length_);
  data_ = nullptr;
  mem_fd_ = -1;
  mem_length_ = 0;
}

u8* subscribe_impl_shm::get_message(NodeError& result, float timeout_sec, size_t* message_size) {
  // This call might block
  unsigned int elem_index;
  result = indices_->get_next_published(bk_, last_index_, elem_index, timeout_sec);

  if (result == SUCCESS) {
    // This assert is a bit suspect, there is a remote corner case where the assert might fail
    assert(last_index_ != static_cast<int32_t>(elem_index));
    last_index_ = elem_index;
    u8* msg = &elems_[elem_index * aligned_elem_size_];
    if (message_size) {
      *message_size = reinterpret_cast<cbuf_preamble*>(msg)->size();
    }
    return msg;
  }
  // The most likely problem here is that the producer died, maybe check one day
  return nullptr;
}

bool subscribe_impl_shm::is_there_new() {
  unsigned int idx = indices_->get_next_index(bk_, last_index_);
  return indices_->is_index_available(bk_, idx);
}

void subscribe_impl_shm::release_message(u32 idx) {
  if (!indices_->release(bk_, idx)) {
    vlog_error(VCAT_NODE,
               "Node %s , subscribed to topic %s held the message too long, this can cause heavy problems. "
               "Resetting indices",
               node_name_.c_str(), topic_name_.c_str());
    reset_message_index();
  }
}

u64 subscribe_impl_shm::get_num_published() const { return indices_->get_num_packets(); }
size_t subscribe_impl_shm::get_num_available() const { return indices_->get_num_indices(bk_, last_index_); }
void subscribe_impl_shm::debug_string(std::string& dbg) {
  char buf[100];
  indices_->sprint_state(bk_, buf, 100);
  dbg = buf;
  dbg += " last_index " + std::to_string(last_index_);
}

static bool try_to_connect(const char host_ips[20][20], const std::string& channel_ip,
                           std::string& connected_ip, uint16_t port, int& sockfd, int timeout_ms,
                           bool blocking_socket) {
  char ipvec[5][20] = {};
  bool connected = false;
  static_split(channel_ip, ';', ipvec);
  sockfd = -1;

  VLOG_ASSERT(host_ips[0] != 0, "Host IPs should be initialized before calling this function");

  // First, let's see if we have an IP on the same subnet
  std::string ip_same_subnet;
  for (int hostidx = 0; hostidx < 20; hostidx++) {
    const char* hostip = host_ips[hostidx];
    if (hostip[0] == 0) break;
    for (auto& clientip : ipvec) {
      if (clientip[0] == 0) break;
      if (same_subnet(hostip, clientip)) {
        ip_same_subnet = clientip;
        break;
      }
    }
    if (!ip_same_subnet.empty()) {
      break;
    }
  }

  if (!ip_same_subnet.empty()) {
    // We found an ip on the same subnet, prefer this one
    auto result = connect_with_timeout(ip_same_subnet, port, sockfd, timeout_ms, blocking_socket);
    if (result == node::NodeError::SUCCESS) {
      connected = true;
      connected_ip = ip_same_subnet;
      return connected;
    } else {
      if (sockfd != -1) close(sockfd);
      sockfd = -1;
      vlog_fine(VCAT_NODE, "Tried connecting to server on ip %s:%u", ip_same_subnet.c_str(), port);
    }
  }

  for (auto& ip : ipvec) {
    if (ip[0] == 0) continue;
    auto result = connect_with_timeout(ip, port, sockfd, timeout_ms, blocking_socket);
    if (result == node::NodeError::SUCCESS) {
      connected = true;
      connected_ip = ip;
      break;
    } else {
      if (sockfd != -1) close(sockfd);
      sockfd = -1;
      vlog_fine(VCAT_NODE, "Tried connecting to publisher on ip %s:%u, will try with next ip", ip, port);
    }
  }

  return connected;
}

NodeError subscribe_impl_network::open_internal(const channel_info& cn_info, u32 num, bool quiet,
                                                float timeout_sec) {
  if (host_ips_[0] == 0) {
    get_host_ips_vec(host_ips_);
  }
  std::string connected_ip;
  if (!try_to_connect(host_ips_, cn_info.channel_ip, connected_ip, cn_info.channel_port, sockfd_,
                      static_cast<int>(1000 * timeout_sec), true)) {
    if (!quiet) {
      vlog_error(VCAT_NODE, "Could not connect to publisher of topic %s, using IPs: %s on port %u",
                 topic_name_.c_str(), cn_info.channel_ip.c_str(), cn_info.channel_port);
    }
    return SOCKET_SERVER_NOT_FOUND;
  }

  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 300000;
  int ret = setsockopt(sockfd_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
  if (ret < 0) {
    perror("Recv timeout");
  }

  //  if (fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL, 0) | O_NONBLOCK) < 0) {
  //    perror("calling fcntl");
  //    return SUBSCRIBER_SOCKET_FCNTL_ERROR;
  //  }

  num_elems_ = num;
  num_published_ = 0;

  // fill in available_buffers
  for (u32 i = 0; i < num_elems_; i++) {
    available_buffers_.emplace(1024);
  }

  vlog_debug(VCAT_NODE, "Opening network subscriber %s socket %d", topic_name_.c_str(), sockfd_);
  return node::SUCCESS;
}

NodeError subscribe_impl_network::open(const channel_info& cn_info, u32 num, bool quiet, float timeout_sec) {
  NodeError res = open_internal(cn_info, num, quiet, timeout_sec);
  if (res != SUCCESS) return res;

  // Create the listening thread here
  quit_thread_ = false;
  VLOG_ASSERT(!th_.joinable());
  th_ = std::thread([this]() { thread_function(); });

  return node::SUCCESS;
}

NodeError subscribe_impl_network::open_notify(const channel_info& cn_info, u32 num_elems,
                                              NotificationManager& nm, NotificationManager::NotifyCallback cb,
                                              bool quiet, float timeout_sec) {
  NodeError res = open_internal(cn_info, num_elems, quiet, timeout_sec);
  if (res != SUCCESS) return res;

  if (pipe2(pipefd_, O_NONBLOCK | O_CLOEXEC) < 0) {
    if (!quiet) {
      vlog_error(VCAT_NODE, "Could not create unnamed pipe for topic %s, error: %d - %s", topic_name_.c_str(),
                 errno, strerror(errno));
    }
    return NodeError::GENERIC_ERROR;
  }

  if (!quiet) {
    vlog_debug(VCAT_NODE, "open_notify network subscriber %s pipefd= %d , %d", topic_name_.c_str(),
               pipefd_[0], pipefd_[1]);
  }

  // vlog_warning(VCAT_NODE, "Topic %s: Socket %d open_notify", topic_name_.data(), pipefd_);
  // Do not create a listening thread here, we use the notification manager
  if (!nm.add_socket_notification(pipefd_[0], cb, topic_name_)) {
    if (!quiet) {
      vlog_error(VCAT_NODE, "Issue adding Notification callback for topic %s", topic_name_.c_str());
    }
    return NodeError::GENERIC_ERROR;
  }

  // This ensures we do not have repeated messages on notifications
  allow_empty_queue_ = true;

  // Create the listening thread here
  quit_thread_ = false;
  VLOG_ASSERT(!th_.joinable());
  th_ = std::thread([this]() { thread_function(); });
  snprintf(thread_name_, sizeof(thread_name_), "ns_%s", topic_name_.c_str());
  pthread_setname_np(th_.native_handle(), thread_name_);

  notification_manager_ = &nm;
  return node::SUCCESS;
}

void subscribe_impl_network::close() {
  if (notification_manager_) {
    notification_manager_->remove_socket_notification(pipefd_[0]);
    notification_manager_ = nullptr;
  }

  quit_thread_ = true;

  if (th_.joinable()) {
    VLOG_ASSERT(th_.get_id() != std::this_thread::get_id());
    th_.join();
  }
  if (sockfd_ >= 0) {
    ::close(sockfd_);
  }
  sockfd_ = -1;
  if (pipefd_[0] >= 0) {
    ::close(pipefd_[0]);
  }
  pipefd_[0] = -1;
  if (pipefd_[1] >= 0) {
    ::close(pipefd_[1]);
  }
  pipefd_[1] = -1;
}

subscribe_impl_network::~subscribe_impl_network() { close(); }

bool subscribe_impl_network::is_there_new() {
  std::lock_guard lock(mtx_);
  return !received_messages_.empty();
}

// Leave only one message
void subscribe_impl_network::reset_message_index() {
  std::lock_guard lock(mtx_);
  VLOG_ASSERT(checkedout_buffer_.empty(),
              "Do not reset indices if there is a checked out message for topic %s on node %s",
              topic_name_.data(), node_name_.data());
  while (received_messages_.size() > 1) {
    auto buf = std::move(received_messages_.front());
    received_messages_.pop();
    available_buffers_.push(std::move(buf));
  }
}

void subscribe_impl_network::release_message(u32 idx) {
  (void)idx;
  std::lock_guard lock(mtx_);
  VLOG_ASSERT(!checkedout_buffer_.empty(), "Cannot release if there is nothing checked out");
  if (!allow_empty_queue_ && received_messages_.empty()) {
    // we should put the buffer at the top of received messages
    received_messages_.push(std::move(checkedout_buffer_));
  } else {
    // return the message to the available buffers
    available_buffers_.push(std::move(checkedout_buffer_));
  }
}

size_t subscribe_impl_network::get_num_available() const {
  std::lock_guard lock(mtx_);
  return received_messages_.size();
}

u8* subscribe_impl_network::get_message(NodeError& result, float timeout_sec, size_t* msg_size) {
  VLOG_ASSERT(checkedout_buffer_.empty(), "Topic %s trying to do 2 consecutive get_message",
              topic_name_.data());
  // check if we have a new message:
  std::unique_lock lock(mtx_);

  if (notification_manager_) {
    // On the notification manager case, we should only be called after a notification, consume
    // the pipe (empty it). Do this inside the lock!
    char c;
    while (::read(pipefd_[0], &c, 1) > 0)
      ;
  }

  if (!received_messages_.empty()) {
    auto buf = std::move(received_messages_.front());
    received_messages_.pop();
    checkedout_buffer_ = std::move(buf);
    result = SUCCESS;
    if (msg_size) *msg_size = checkedout_buffer_.size();
    return checkedout_buffer_.data();
  }

  // if not, check for possible thread issues, errors
  if (quit_thread_) {
    vlog_error(VCAT_NODE, "Trying to get a message when the publisher is disconnected");
    result = PRODUCER_NOT_PRESENT;
    return nullptr;
  }

  if (!notification_manager_) {
    using namespace std::chrono_literals;
    // wait until the thread has received a new message, use condition variable
    cond_v_.wait_for(lock, timeout_sec * 1000ms, [&] { return !received_messages_.empty(); });
  }

  // If received_messages is empty, we had nothing to read
  if (received_messages_.empty()) {
    result = NO_MESSAGE_AVAILABLE;
    return nullptr;
  }

  // success!
  checkedout_buffer_ = std::move(received_messages_.front());
  received_messages_.pop();
  result = SUCCESS;
  if (msg_size) *msg_size = checkedout_buffer_.size();
  return checkedout_buffer_.data();
}

// This function will process all messages and not accept new ones while this is happening
NodeError subscribe_impl_network::get_all_messages(std::function<void(u8*, size_t)> fn) {
  VLOG_ASSERT(checkedout_buffer_.empty(), "Topic %s trying to do 2 consecutive get_message",
              topic_name_.data());
  // check if we have a new message:
  std::lock_guard lock(mtx_);
  while (!received_messages_.empty()) {
    auto buf = std::move(received_messages_.front());
    received_messages_.pop();
    fn(buf.data(), buf.size());
    available_buffers_.push(std::move(buf));
  }
  return SUCCESS;
}

bool subscribe_impl_network::read_preamble_from_socket(cbuf_preamble& pre) {
  ssize_t bytes = 0;
  bytes = recv(sockfd_, &pre, sizeof(pre), MSG_PEEK | MSG_WAITALL);
  if (quit_thread_) return false;

  if ((bytes == -1) && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
    return false;
  }

  if (bytes < 0) {
    vlog_warning(VCAT_NODE, "Node network %s: Socket %d receiving cbuf preamble errno: %s", node_name_.data(),
                 sockfd_, strerror(errno));
    // Mark this as an error
    preclose();
    return false;
  }

  if (bytes != sizeof(pre)) {
    if (bytes == 0) {
      vlog_debug(VCAT_NODE,
                 "Node : %s --> Publisher of the network topic %s has disconnected. Likely terminating",
                 node_name_.data(), topic_name_.data());
    } else {
      vlog_warning(VCAT_NODE,
                   "Node : %s --> Publisher of the network topic %s has disconnected. [received %zd bytes, "
                   "expected %zu bytes]",
                   node_name_.data(), topic_name_.data(), bytes, sizeof(pre));
    }
    preclose();
    return false;
  }

  if (pre.magic != CBUF_MAGIC) {
    vlog_error(VCAT_NODE,
               "Node: %s -> Topic %s has sent non coherent data, cbuf_magic 0x%X but expected 0x%X. "
               "Terminating socket",
               node_name_.data(), topic_name_.data(), pre.magic, CBUF_MAGIC);
    preclose();
    return false;
  }

  return true;
}

bool subscribe_impl_network::read_cbuf_from_socket(u8* data, size_t sz_to_read) {
  size_t bytes_received = 0;
  ssize_t bytes;
  while (bytes_received < sz_to_read) {
    uint8_t* ptr = data + bytes_received;
    size_t to_receive = sz_to_read - bytes_received;
    bytes = recv(sockfd_, ptr, to_receive, MSG_WAITALL);
    if (quit_thread_) return false;

    if (bytes < 0) {
      if (errno == EAGAIN || errno == EINTR) {
        // for very large messages, we could have had an interrupt, just keep at it
        continue;
      }
      // we had a bad error. TODO: handle this better
      vlog_error(
          VCAT_NODE,
          "Error, topic %s had errno: %d - %s reading cbuf from socket!! Asked for %zu bytes and got %zd\n",
          topic_name_.c_str(), errno, strerror(errno), sz_to_read, bytes);
      preclose();
      return false;
    }
    bytes_received += bytes;
  }

  return true;
}

// this function returns false if there was an error
bool subscribe_impl_network::read_message_from_socket() {
  cbuf_preamble pre;
  bool new_msg = false;
  bool do_checksum = false;
  node::oob_msg oob;

  while (!new_msg) {
    pre = {};
    if (!read_preamble_from_socket(pre)) {
      return false;
    }

    // This might be an out of band message!
    if (pre.hash == node::oob_msg::TYPE_HASH) {
      // Handle the OOB msg
      if (!read_cbuf_from_socket((u8*)&oob, pre.size())) {
        return false;
      }
      // The only OOB we support now is hash verification, keep track of that here.
      do_checksum = true;
    } else {
      // Process the message
      new_msg = true;
    }
  }

  // Get an available buffer
  std::vector<u8> buffer;
  {
    std::lock_guard lk(mtx_);
    if (available_buffers_.empty()) {
      if (!received_messages_.empty()) {
        // Take the oldest received if nothing is checked out
        buffer = std::move(received_messages_.front());
        received_messages_.pop();
      } else {
        // this should never happen
        vlog_fatal(VCAT_NODE,
                   "Run out of buffers for network subscriber, node: %s, topic: %s. Main thread has "
                   "checked out a message. Cbuf size: %u",
                   node_name_.data(), topic_name_.data(), pre.size());
      }
    } else {
      buffer = std::move(available_buffers_.front());
      available_buffers_.pop();
    }
  }

  buffer.resize(pre.size());
  if (!read_cbuf_from_socket(buffer.data(), pre.size())) {
    return false;
  }

  if (do_checksum) {
    // We had a checksum provided, ensure they match!
    uint32_t crc = crc32buf(buffer.data(), buffer.size());
    VLOG_ASSERT(oob.crc == crc, "Topic %s failed to match hash!", topic_name_.c_str());
  }
  // We got a valid message, put it out
  {
    std::lock_guard lk(mtx_);
    received_messages_.push(std::move(buffer));
    num_published_++;
  }

  return true;
}

void subscribe_impl_network::thread_function() {
  while (!quit_thread_) {
    if (!read_message_from_socket()) continue;
    if (notification_manager_) {
      char c = 1;
      const auto written_n = ::write(pipefd_[1], &c, 1);
      assert(written_n == 1);
      (void)written_n;
    } else {
      cond_v_.notify_all();
    }
  }
}

void publisher_impl_network::thread_function() {
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);
  snprintf(thread_name_, sizeof(thread_name_), "npub_%s", topic_name_.c_str());
  pthread_setname_np(pthread_self(), thread_name_);

  int poll_status;
  while (accept_new_clients_) {
    pollfd pfd;
    pfd.fd = listen_socket_;
    pfd.events = POLLIN;
    int timeout = 1000;  // 1 second timeout

    poll_status = poll(&pfd, 1, timeout);
    if (poll_status < 0) {
      vlog_error(VCAT_NODE, "Poll on Publisher socket errored out: %s", strerror(errno));
    } else if (poll_status == 0) {
      // timed out, loop again
    } else {
      if (pfd.revents == POLLNVAL) {
        vlog_error(VCAT_NODE, "Poll on publisher returned but the event was POLLNVAL, socket: %d",
                   listen_socket_);
        continue;
      }
      if (pfd.revents != POLLIN) {
        vlog_error(VCAT_NODE, "Poll on publisher returned but the event was not pollin: %d", pfd.revents);
        continue;
      }
      int new_socket = accept4(listen_socket_, (struct sockaddr*)&address, &addrlen, SOCK_CLOEXEC);
      if (new_socket < 0) {
        vlog_error(VCAT_NODE, "Publisher for topic %s had error doing accept: %d - %s", topic_name_.c_str(),
                   errno, strerror(errno));
      } else {
        int opt = 1;
        int ret = setsockopt(new_socket, IPPROTO_TCP, TCP_NODELAY, (void*)&opt, sizeof(opt));
        if (ret < 0) {
          vlog_error(VCAT_NODE, "Publisher for topic %s had error setting socket tcpnodelay: %d - %s",
                     topic_name_.c_str(), errno, strerror(errno));
        }

        struct timeval s_timeout;
        s_timeout.tv_sec = timeout_ms_ / 1000L;
        s_timeout.tv_usec = (timeout_ms_ % 1000L) * 1000L;

        ret = setsockopt(new_socket, SOL_SOCKET, SO_SNDTIMEO, (char*)&s_timeout, sizeof(s_timeout));
        if (ret < 0) {
          vlog_error(VCAT_NODE, "Publisher for topic %s had error setting socket send timeout: %d - %s",
                     topic_name_.c_str(), errno, strerror(errno));
        }

        std::lock_guard lk(mtx_);
        socket_clients_.insert(new_socket);
      }
    }
  }
}

NodeError publisher_impl_network::open(const topic_info* info) {
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);
  (void)info;

  // Creating socket file descriptor
  if ((listen_socket_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0)) < 0) {
    vlog_error(VCAT_NODE, "Socket creation for publisher network failed with errno %d: %s", errno,
               strerror(errno));
    return PUBLISHER_SOCKET_COULD_NOT_BE_CREATED;
  }

  // Forcefully attaching socket to the port
  // Do not specify REUSEPORT, this can cause problems when there are multiple
  // nodemasters
  //  if (setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR,  //| SO_REUSEPORT,
  //                 &opt, sizeof(opt))) {
  //    perror("setsockopt");
  //    return false;
  //  }
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = 0;

  // Forcefully attaching socket to the port 8080
  if (bind(listen_socket_, (struct sockaddr*)&address, sizeof(address)) < 0) {
    perror("bind failed");
    return PUBLISHER_SOCKET_BIND_ERROR;
  }

  if (getsockname(listen_socket_, (struct sockaddr*)&address, &addrlen) < 0) {
    perror("getsockname failed");
  }

  port_ = ntohs(address.sin_port);
  host_ip_ = get_host_ips();

  if (listen(listen_socket_, 5) < 0) {
    perror("listen");
    return PUBLISHER_SOCKET_LISTEN_ERROR;
  }

  accept_new_clients_ = true;
  topic_name_ = info->topic_name;
  timeout_ms_ = info->cn_info.timeout_ms;
  vlog_debug(VCAT_NODE, "Open publisher network %s at socket %d", topic_name_.c_str(), listen_socket_);
  listener_ = std::thread([this]() { this->thread_function(); });

  return SUCCESS;
}

bool publisher_impl_network::send_bytes_internal(const u8* bytes, size_t size, int sk) {
  size_t ret = 0;
  bool error = false;
  double start_transfer_time = time_now();
  while (ret < size) {
    auto wbytes = send(sk, bytes + ret, size - ret, MSG_NOSIGNAL);
    if (wbytes == -1) {
      if (errno == EWOULDBLOCK || errno == EAGAIN) {
        double now = time_now();
        // We use a non blocking socket, skip this for now
        vlog_warning(VCAT_NODE,
                     "Publisher on topic %s would block on socket %d, skipping this message for this client "
                     "and disconnecting. Transferred %zu bytes out of %zu, and took %.2fms to timeout",
                     topic_name_.c_str(), sk, ret, size, (now - start_transfer_time) * 1000.0);
      } else if (errno == ECONNRESET || errno == EPIPE) {
        vlog_debug(VCAT_NODE, "Client of topic %s disconnected", topic_name_.c_str());
      } else {
        // This could be made better to handle other errors
        vlog_warning(VCAT_NODE,
                     "Error errno [%s] (%d) on writing to socket: %d, topic: %s, will disconnect client "
                     "(bytes internal)",
                     strerror(errno), errno, sk, topic_name_.c_str());
      }
      error = true;
      break;
    } else {
      ret += wbytes;
    }
  }
  return error;
}

bool publisher_impl_network::transmit_message(const u8* bytes, size_t size) {
  // Possible optimization, just copy the vector of socket_clients to keep the lock
  // as little as possible
  std::set<int> sockets;
  {
    std::lock_guard lk(mtx_);
    sockets = socket_clients_;
  }
  node::oob_msg oob;
  if (use_checksum_) {
    oob.crc = crc32buf(bytes, size);
  }

  for (auto it = sockets.begin(); it != sockets.end();) {
    int sk = *it;
    bool error = false;

    if (use_checksum_) {
      error = send_bytes_internal((u8*)&oob, oob.encode_size(), sk);
    }

    if (!error) error = send_bytes_internal(bytes, size, sk);

    if (error) {
      std::lock_guard lk(mtx_);
      socket_clients_.erase(sk);
      ::close(sk);
    }

    vlog_fine(VCAT_NODE, "Transmission of message for topic %s to client socket %d: %s", topic_name_.c_str(),
              sk, error ? "ERROR" : "SUCCESS");
    it++;
  }
  num_published_++;
  return true;
}

void publisher_impl_network::fill_channel_info(topic_info& info) {
  info.cn_info.is_network = true;
  info.cn_info.channel_ip = get_host_ip();
  info.cn_info.channel_port = get_port();
}

void publisher_impl_network::close() {
  accept_new_clients_ = false;
  if (listener_.joinable()) {
    listener_.join();
  }

  if (listen_socket_ >= 0) {
    ::close(listen_socket_);
    listen_socket_ = -1;
  }
  for (auto cl : socket_clients_) {
    if (cl >= 0) {
      ::close(cl);
    }
  }
  socket_clients_.clear();
}

void observer::release_message(MsgBufPtr& msg) {
  impl_->release_message(msg.getIdx());
  msg.make_empty();
}

// this function will open the socket and fill the information if successful
NodeError response_provider_base::open(topic_info* info) {
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);

  // Creating socket file descriptor
  if ((listen_socket_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0)) < 0) {
    vlog_error(VCAT_NODE, "Socket creation for response provider failed with errno %d: %s", errno,
               strerror(errno));
    return PUBLISHER_SOCKET_COULD_NOT_BE_CREATED;
  }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = 0;

  if (bind(listen_socket_, (struct sockaddr*)&address, sizeof(address)) < 0) {
    perror("bind failed");
    return PUBLISHER_SOCKET_BIND_ERROR;
  }

  if (getsockname(listen_socket_, (struct sockaddr*)&address, &addrlen) < 0) {
    perror("getsockname failed");
  }

  port_ = ntohs(address.sin_port);
  host_ip_ = get_host_ips();

  if (listen(listen_socket_, 5) < 0) {
    perror("listen");
    return PUBLISHER_SOCKET_LISTEN_ERROR;
  }

  info->cn_info.channel_ip = host_ip_;
  info->cn_info.is_network = true;
  info->cn_info.channel_port = port_;

  accept_new_clients_ = true;
  topic_name_ = info->topic_name;
  listener_ = std::thread([this]() { this->thread_function(); });

  vlog_info(VCAT_NODE, "Opened Response provider on %s:%d", host_ip_.c_str(), port_);
  return SUCCESS;
}

NodeError response_provider_base::open_notify(topic_info* info, NotificationManager& nm) {
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);

  // Creating socket file descriptor
  if ((listen_socket_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0)) < 0) {
    vlog_error(VCAT_NODE, "Socket creation for response provider notify failed with errno %d: %s", errno,
               strerror(errno));
    return PUBLISHER_SOCKET_COULD_NOT_BE_CREATED;
  }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = 0;

  if (bind(listen_socket_, (struct sockaddr*)&address, sizeof(address)) < 0) {
    perror("bind failed");
    return PUBLISHER_SOCKET_BIND_ERROR;
  }

  if (getsockname(listen_socket_, (struct sockaddr*)&address, &addrlen) < 0) {
    perror("getsockname failed");
  }

  port_ = ntohs(address.sin_port);
  host_ip_ = get_host_ips();

  if (listen(listen_socket_, 5) < 0) {
    perror("listen");
    return PUBLISHER_SOCKET_LISTEN_ERROR;
  }

  info->cn_info.channel_ip = host_ip_;
  info->cn_info.is_network = true;
  info->cn_info.channel_port = port_;

  accept_new_clients_ = true;
  topic_name_ = info->topic_name;
  nm_ = &nm;
  nm_->add_socket_notification(
      listen_socket_, [this](int fd) { this->handle_new_connection(fd); }, topic_name_);

  vlog_info(VCAT_NODE, "Opened Response provider on %s:%d", host_ip_.c_str(), port_);
  return SUCCESS;
}

void response_provider_base::close() {
  accept_new_clients_ = false;
  if (listener_.joinable()) listener_.join();
  if (listen_socket_ != -1) {
    if (nm_ != nullptr) {
      nm_->remove_socket_notification(listen_socket_);
    }
    ::close(listen_socket_);
    listen_socket_ = -1;
  }
  for (auto client_fd : socket_clients_) {
    if (nm_ != nullptr) {
      nm_->remove_socket_notification(client_fd);
    }
    ::close(client_fd);
  }
  socket_clients_.clear();
}

bool response_provider_base::handle_incoming_message(int client_fd) {
  bool message_read = false;
  bool internal_request = false;
  while (!message_read) {
    cbuf_preamble pre = {};
    ssize_t bytes = 0;
    bytes = recv(client_fd, &pre, sizeof(pre), MSG_PEEK | MSG_WAITALL);
    if ((bytes == -1) && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      continue;
    }
    if (bytes < 0) {
      vlog_warning(VCAT_NODE, "Node Response provider for %s: Socket %d receiving cbuf preamble errno: %s",
                   topic_name_.data(), client_fd, strerror(errno));
      // Mark this as an error
      ::close(client_fd);
      socket_clients_.erase(client_fd);
      return false;
    }

    if (bytes != sizeof(pre)) {
      if (bytes == 0) {
        vlog_debug(VCAT_NODE, "Node Requester for %s has disconnected. Likely client termination",
                   topic_name_.data());

      } else {
        vlog_warning(VCAT_NODE,
                     "Node Requester for %s has disconnected. [received %zd bytes, "
                     "expected %zu bytes]",
                     topic_name_.data(), bytes, sizeof(pre));
      }
      ::close(client_fd);
      socket_clients_.erase(client_fd);
      return false;
    }

    // ensure our buffer has enough space
    receiving_buffer_.resize(pre.size());

    size_t bytes_received = 0;
    while (bytes_received < pre.size()) {
      uint8_t* ptr = receiving_buffer_.data() + bytes_received;
      size_t to_receive = pre.size() - bytes_received;
      bytes = recv(client_fd, ptr, to_receive, MSG_WAITALL);

      if (bytes < 0) {
        if (errno == EAGAIN || errno == EINTR) {
          // for very large messages, we could have had an interrupt, just keep at it
          continue;
        }
        // we had a bad error. TODO: handle this better
        vlog_error(VCAT_NODE,
                   "Error, errno: [%s] reading cbuf from socket!! Will disconnect. Asked for %zu bytes and "
                   "got %zd\n",
                   strerror(errno), receiving_buffer_.size(), bytes);
        ::close(client_fd);
        socket_clients_.erase(client_fd);
        return false;
      }
      bytes_received += bytes;
    }
    message_read = true;
    if (pre.hash == rpc_internal_request::TYPE_HASH) {
      internal_request = true;
    }
  }

  size_t transmitting_size = 0;
  u8* bytes = nullptr;
  rpc_internal_reply internal_reply;
  NodeError res;

  if (internal_request) {
    // Handle internal request
    rpc_internal_request intreq;
    if (!intreq.decode_net((char*)receiving_buffer_.data(), (int)receiving_buffer_.size())) {
      internal_reply.return_code = NodeError::IDL_DECODE_ERROR;
    } else {
      internal_reply.return_code = NodeError::SUCCESS;
      if (intreq.type == RpcInternalRequestType::NUM_REQUESTS_HANDLED) {
        internal_reply.num_requests_handled = (u32)num_requests_handled_;
      } else if (intreq.type == RpcInternalRequestType::TERMINATE_RPC) {
        vlog_debug(VCAT_NODE, "Closing client of %s at the client request", topic_name_.data());
        ::close(client_fd);
        socket_clients_.erase(client_fd);
        return false;
      } else {
        internal_reply.return_code = NodeError::GENERIC_ERROR;
      }
    }
    transmitting_size = internal_reply.encode_size();
    bytes = (u8*)&internal_reply;

  } else {
    // We got a valid message, do the callback work
    res = handleMsg();

    // Now send the reply back
    if (res != SUCCESS) {
      internal_reply.return_code = res;
      transmitting_size = internal_reply.encode_size();
      bytes = (u8*)&internal_reply;
    } else {
      transmitting_size = transmitting_buffer_.size();
      bytes = transmitting_buffer_.data();
    }
  }

  size_t sent_bytes = 0;
  while (sent_bytes < transmitting_size) {
    auto wbytes = send(client_fd, bytes + sent_bytes, transmitting_size - sent_bytes, MSG_NOSIGNAL);
    if (wbytes == -1) {
      // This could be made better to handle other errors
      vlog_warning(VCAT_NODE,
                   "Error errno [%s] (%d) on writing to socket: %d, topic: %s, will disconnect client "
                   "(handle incoming msg)",
                   strerror(errno), errno, client_fd, topic_name_.c_str());
      ::close(client_fd);
      socket_clients_.erase(client_fd);
      return false;
    } else {
      sent_bytes += wbytes;
    }
  }

  // do not count internal request in the number handled
  if (!internal_request) num_requests_handled_++;
  return true;
}

void response_provider_base::handle_new_connection(int fd) {
  VLOG_ASSERT(fd == listen_socket_);
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);
  int new_socket = accept4(listen_socket_, (struct sockaddr*)&address, &addrlen, SOCK_CLOEXEC);
  if (new_socket < 0) {
    perror("accept on new client");
    return;
  }

  int opt = 1;
  int ret = setsockopt(new_socket, IPPROTO_TCP, TCP_NODELAY, (void*)&opt, sizeof(opt));
  if (ret < 0) {
    perror("Setting TCP_NODELAY");
  }
  socket_clients_.insert(new_socket);
  if (nm_ != nullptr) {
    nm_->add_socket_notification(
        new_socket,
        [this](int lambda_fd) {
          if (!this->handle_incoming_message(lambda_fd)) {
            nm_->remove_socket_notification(lambda_fd);
            socket_clients_.erase(lambda_fd);
            ::close(lambda_fd);
          }
        },
        topic_name_);
  }
}

void response_provider_base::thread_function() {
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);
  snprintf(thread_name_, sizeof(thread_name_), "rpcprov_%s", topic_name_.c_str());
  pthread_setname_np(pthread_self(), thread_name_);

  int poll_status;
  std::vector<pollfd> pfds;
  pfds.reserve(20);
  while (accept_new_clients_) {
    // relatively tame number of concurrent users...
    pfds.resize(1 + socket_clients_.size());
    pfds[0].fd = listen_socket_;
    pfds[0].events = POLLIN;

    size_t idx = 1;
    for (auto client_fd : socket_clients_) {
      pfds[idx].fd = client_fd;
      pfds[idx].events = POLLIN;
      idx++;
    }

    int timeout = 1000;  // 1 second timeout

    poll_status = poll(pfds.data(), pfds.size(), timeout);
    if (poll_status < 0) {
      vlog_error(VCAT_NODE, "Poll on Response Provider socket errored out: %s", strerror(errno));
    } else if (poll_status == 0) {
      // timed out, loop again
    } else {
      for (idx = 1; idx < pfds.size(); idx++) {
        if ((pfds[idx].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
          vlog_debug(VCAT_NODE, "Requester on fd %d disconnected", pfds[idx].fd);
          ::close(pfds[idx].fd);
          socket_clients_.erase(pfds[idx].fd);
        } else if ((pfds[idx].revents & POLLIN) == POLLIN) {
          // Handle the message
          handle_incoming_message(pfds[idx].fd);
        }
      }
      if ((pfds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        vlog_error(VCAT_NODE, "Poll on requester listener returned but the event was not pollin: %d",
                   pfds[0].revents);
        continue;
      }

      if ((pfds[0].revents & POLLIN) == POLLIN) {
        int new_socket = accept4(listen_socket_, (struct sockaddr*)&address, &addrlen, SOCK_CLOEXEC);
        if (new_socket < 0) {
          perror("accept on new client");
        } else {
          int opt = 1;
          int ret = setsockopt(new_socket, IPPROTO_TCP, TCP_NODELAY, (void*)&opt, sizeof(opt));
          if (ret < 0) {
            perror("Setting TCP_NODELAY");
          }

          socket_clients_.insert(new_socket);
        }
      }
    }
  }
  vlog_info(VCAT_NODE, "Thread for Response Provider on topic %s terminating...", topic_name_.c_str());
}

// This function opens a socket
NodeError requester_base::open(float timeout_sec, float retry_delay_sec, const std::string& channel_ip,
                               uint16_t port) {
  timeout_sec_ = timeout_sec;
  retry_delay_sec_ = retry_delay_sec;
  channel_ip_ = channel_ip;
  channel_port_ = port;
  // Reset our counter for disconnection internal messages
  close_request_sent_ = false;
  expect_reply_ = true;

  if (host_ips_[0][0] == 0) {
    get_host_ips_vec(host_ips_);
  }

  VLOG_ASSERT(sockfd_ == -1);
  if (!try_to_connect(host_ips_, channel_ip_, connected_ip_, channel_port_, sockfd_, int(timeout_sec_ * 1000),
                      !using_notification_manager_)) {
    vlog_error(VCAT_NODE, "Could not connect to provider, using IPs: %s on port %u", channel_ip_.c_str(),
               channel_port_);
    return SOCKET_SERVER_NOT_FOUND;
  } else {
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = recv_timeout_ms * 1000;
    int ret = setsockopt(sockfd_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
    if (ret < 0) {
      perror("Recv timeout");
    }
  }
  return SUCCESS;
}

NodeError requester_base::open_notify(float timeout_sec, float retry_delay_sec, const std::string& channel_ip,
                                      uint16_t port, NotificationManager& nm) {
  using_notification_manager_ = true;
  NodeError res;
  res = open(timeout_sec, retry_delay_sec, channel_ip, port);
  if (res != SUCCESS) return res;

  NotificationManager::NotifyCallback notifycb = [&](int fd) {
    (void)fd;
    auto result = receiveReply(true);
    if (result == SUCCESS) {
      handleAsyncCallback();
    }
  };

  if (!nm.add_socket_notification(sockfd_, notifycb, topic_name_)) {
    vlog_error(VCAT_NODE, "Issue adding Notification callback for topic %s", topic_name_.c_str());
    return NodeError::GENERIC_ERROR;
  }
  return node::SUCCESS;
}

NodeError requester_base::sendRequestOnly() {
  size_t transmitting_size = transmitting_buffer_.size();
  u8* bytes = transmitting_buffer_.data();
  rpc_internal_reply internal_reply;

  if (sockfd_ == -1) {
    // we got disconnect, hammer again
    // now this only tries socket reconnect, no whole node reconnect
    NodeError res = requester_base::open(timeout_sec_, retry_delay_sec_, channel_ip_, channel_port_);
    if (res != SUCCESS) {
      return res;
    }
  }

  size_t sent_bytes = 0;
  while (sent_bytes < transmitting_size) {
    auto wbytes = send(sockfd_, bytes + sent_bytes, transmitting_size - sent_bytes, MSG_NOSIGNAL);
    if (wbytes == -1) {
      // This could be made better to handle other errors
      vlog_warning(
          VCAT_NODE,
          "Error errno [%s] (%d) on writing to socket: %d, topic: %s, will disconnect client (sendRequest)",
          strerror(errno), errno, sockfd_, topic_name_.c_str());
      close_cleanup();
      return SOCKET_WRITE_ERROR;
    } else {
      sent_bytes += wbytes;
    }
  }
  return SUCCESS;
}

NodeError requester_base::receiveReply(bool async) {
  bool message_read = false;
  double start_read_time = time_now();
  double too_late = start_read_time + double(recv_timeout_ms) / 1000.0;
  while (!message_read) {
    cbuf_preamble pre = {};
    ssize_t bytes = 0;
    bytes = recv(sockfd_, &pre, sizeof(pre), MSG_PEEK | MSG_WAITALL);
    if ((bytes == -1) && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
      if (time_now() > too_late) {
        vlog_warning(VCAT_NODE,
                     "Timed out when waiting for a reply from the responder (at pre), sock %d, elapsed: %f",
                     sockfd_, (time_now() - start_read_time));
        // Mark this as an error
        close_cleanup();
        return SOCKET_READ_ERROR;
      }
      continue;
    }
    if (bytes < 0) {
      vlog_warning(VCAT_NODE, "Node Requester for %s: Socket %d receiving cbuf preamble errno: %s",
                   topic_name_.c_str(), sockfd_, strerror(errno));
      // Mark this as an error
      close_cleanup();
      return SOCKET_READ_ERROR;
    }

    if (bytes != sizeof(pre)) {
      vlog_warning(VCAT_NODE,
                   "Node Requester for %s has disconnected. [received %zd bytes, "
                   "expected %zu bytes]",
                   topic_name_.c_str(), bytes, sizeof(pre));
      close_cleanup();
      return SOCKET_READ_ERROR;
    }

    if (pre.magic != CBUF_MAGIC) {
      vlog_error(VCAT_NODE, "Read bad cbuf pre Pre Size %u, Magic %X , Hash %lX on receiving, disconnecting",
                 pre.size(), pre.magic, pre.hash);
      close_cleanup();
      return SOCKET_READ_ERROR;
    }

    // ensure our buffer has enough space
    std::vector<u8>& rec_buffer = async ? receiving_buffer_async_ : receiving_buffer_;
    rec_buffer.resize(pre.size());

    size_t bytes_received = 0;
    while (bytes_received < pre.size()) {
      uint8_t* ptr = rec_buffer.data() + bytes_received;
      size_t to_receive = pre.size() - bytes_received;
      bytes = recv(sockfd_, ptr, to_receive, MSG_WAITALL);

      if (bytes < 0) {
        if (errno == EAGAIN || errno == EINTR) {
          // for very large messages, we could have had an interrupt, just keep at it
          if (time_now() > too_late) {
            vlog_warning(VCAT_NODE, "Timed out when waiting for a reply from the responder (after pre)");
            // Mark this as an error
            close_cleanup();
            return SOCKET_READ_ERROR;
          }
          continue;
        }
        // we had a bad error. TODO: handle this better
        vlog_error(VCAT_NODE,
                   "Error, errno: %d reading cbuf from socket!! Will disconnect. Asked for %zu bytes and "
                   "got %zd\n",
                   errno, rec_buffer.size(), bytes);
        close_cleanup();
        return SOCKET_READ_ERROR;
      }
      bytes_received += bytes;
    }
    message_read = true;

    if (pre.hash == node::rpc_internal_reply::TYPE_HASH) {
      // this is an internal message, usually of some issue
      node::rpc_internal_reply internal_reply;
      if (!internal_reply.decode((char*)rec_buffer.data(), pre.size())) {
        return IDL_DECODE_ERROR;
      }
      return (NodeError)internal_reply.return_code;

      // in the future we could even keep trying to receive messages if needed
    }
  }

  return SUCCESS;
}

NodeError requester_base::sendRequest() {
  // first send our message
  NodeError result = sendRequestOnly();
  if (result != SUCCESS) return result;

  // Nothing more to do, this can be a termination internal message
  if (!expect_reply_) return SUCCESS;

  // now we wait for the reply
  result = receiveReply();
  return result;
}

int requester_base::total_published_messages() {
  rpc_internal_request intreq;
  intreq.type = RpcInternalRequestType::NUM_REQUESTS_HANDLED;
  transmitting_buffer_.resize(intreq.encode_net_size());
  if (!intreq.encode_net((char*)transmitting_buffer_.data(), (u32)transmitting_buffer_.size())) {
    vlog_error(VCAT_NODE, "Error on trying to find number of published messages, could not encode request");
    return -1;
  }

  expect_reply_ = true;
  NodeError res = sendRequest();
  if (res != SUCCESS) {
    vlog_error(VCAT_NODE, "Error on trying to find number of published messages: %s", NodeErrorToStr(res));
    return -1;
  }

  node::rpc_internal_reply internal_reply;
  // Has to work
  VLOG_ASSERT(internal_reply.decode((char*)receiving_buffer_.data(), (u32)internal_reply.encode_net_size()),
              "Has to work since we get here");
  return internal_reply.num_requests_handled;
}

void requester_base::handleAsyncCallback() {
  VLOG_ASSERT(false, "Never call async functions on a pure base requester, not supported");
}

void requester_base::close_cleanup() {
  if (sockfd_ != -1) ::close(sockfd_);
  sockfd_ = -1;
  connected_ip_.clear();
}

void requester_base::close() {
  if (!close_request_sent_ && is_open()) {
    close_request_sent_ = true;
    rpc_internal_request intreq;
    intreq.type = RpcInternalRequestType::TERMINATE_RPC;
    transmitting_buffer_.resize(intreq.encode_net_size());
    if (intreq.encode_net((char*)transmitting_buffer_.data(), (u32)transmitting_buffer_.size())) {
      expect_reply_ = false;
      sendRequest();
    }
  }
  close_cleanup();
}

NodeError observer::open(float timeout_sec, float retry_delay_sec, bool quiet) {
  NodeError res;
  // Find the registry, inquire about this channel
  topic_info info;

  assert(retry_delay_sec >= 0.0 && retry_delay_sec <= 60.0);

  res = get_topic_info(topic_name_, info);
  if (res == SOCKET_SERVER_NOT_FOUND) {
    if (feq(timeout_sec, NODE_WAIT_FOREVER)) {
      if (!quiet) {
        vlog_error(VCAT_NODE, "Failure to find the node master when subscribing for topic %s",
                   topic_name_.c_str());
      }
    }
    return res;
  }
  if (res == TOPIC_NOT_FOUND && !feq(timeout_sec, NODE_NO_RETRY)) {
    float remaining_sec = timeout_sec;
    // printf("Topic %s not found.  Retrying.", topic_name.c_str());
    while (res == TOPIC_NOT_FOUND && (feq(timeout_sec, NODE_WAIT_FOREVER) || remaining_sec > 0)) {
      // printf(".");
      // fflush(stdout);
      usleep(static_cast<useconds_t>(retry_delay_sec * 1e6));
      remaining_sec -= retry_delay_sec;
      res = get_topic_info(topic_name_, info);
    }
    // printf("\n");
  }
  if (res != SUCCESS) {
    // Consumer cannot create new topics
    if (feq(timeout_sec, NODE_WAIT_FOREVER)) {
      // fprintf(stderr, "Failure to find the topic %s in the node registry\n",
      //        topic_name_.c_str());
    }
    if (res == TOPIC_NOT_FOUND) {
      return PRODUCER_NOT_PRESENT;
    }
    return res;
  }

  if (info.type == TopicType::RPC) {
    impl_ = new rpcsubs_impl();
  } else if (info.type == TopicType::PUB_SUB) {
    if (info.cn_info.is_network) {
      impl_ = new subscribe_impl_network();
    } else {
      // Set the aligned elem size to 0 to take it from the producer
      impl_ = new subscribe_impl_shm(0);
    }
  } else if (info.type == TopicType::SINK_SRC) {
    impl_ = new sourcesubs_impl();
  } else {
    vlog_error(VCAT_NODE, "Unknown topic type for %s", topic_name_.c_str());
    return GENERIC_ERROR;
  }
  impl_->set_topic_name(topic_name_);

  res = impl_->open(info.cn_info, info.num_elems);
  if (res != SUCCESS) {
    if (!quiet) {
      vlog_error(VCAT_NODE, "Observer failure to open the topic %s: %s", topic_name_.c_str(),
                 NodeErrorToStr(res));
    }
  }

  msg_type_ = info.msg_name;
  msg_cbuftxt_ = info.msg_cbuftxt;
  msg_hash_ = info.msg_hash;
  return res;
}

NodeError observer::open_notify(NotificationManager& nm, std::function<void(node::MsgBufPtr&)> cb,
                                float timeout_nm_sec, float timeout_connect_sec, float retry_delay_sec,
                                bool quiet) {
  NodeError res;
  // Find the registry, inquire about this channel
  topic_info info;

  assert(retry_delay_sec >= 0.0 && retry_delay_sec <= 60.0);

  res = get_topic_info(topic_name_, info);
  if (res == SOCKET_SERVER_NOT_FOUND) {
    if (feq(timeout_nm_sec, NODE_WAIT_FOREVER)) {
      if (!quiet) {
        vlog_error(VCAT_NODE, "Failure to find the node master when subscribing for topic %s",
                   topic_name_.c_str());
      }
    }
    return res;
  }
  if (res == TOPIC_NOT_FOUND && !feq(timeout_nm_sec, NODE_NO_RETRY)) {
    float remaining_sec = timeout_nm_sec;
    // printf("Topic %s not found.  Retrying.", topic_name.c_str());
    while (res == TOPIC_NOT_FOUND && (feq(timeout_nm_sec, NODE_WAIT_FOREVER) || remaining_sec > 0)) {
      // printf(".");
      // fflush(stdout);
      usleep(static_cast<useconds_t>(retry_delay_sec * 1e6));
      remaining_sec -= retry_delay_sec;
      res = get_topic_info(topic_name_, info);
    }
    // printf("\n");
  }
  if (res != SUCCESS) {
    // Consumer cannot create new topics
    if (feq(timeout_nm_sec, NODE_WAIT_FOREVER)) {
      // fprintf(stderr, "Failure to find the topic %s in the node registry\n",
      //        topic_name_.c_str());
    }
    if (res == TOPIC_NOT_FOUND) {
      return PRODUCER_NOT_PRESENT;
    }
    return res;
  }

  if (info.type == TopicType::RPC) {
    impl_ = new rpcsubs_impl();
  } else if (info.type == TopicType::PUB_SUB) {
    if (info.cn_info.is_network) {
      impl_ = new subscribe_impl_network();
    } else {
      // Set the aligned elem size to 0 to take it from the producer
      impl_ = new subscribe_impl_shm(0);
    }
  } else if (info.type == TopicType::SINK_SRC) {
    impl_ = new sourcesubs_impl();
  } else {
    vlog_error(VCAT_NODE, "Unknown topic type for %s", topic_name_.c_str());
    return GENERIC_ERROR;
  }
  impl_->set_topic_name(topic_name_);

  message_callback_ = cb;

  res = impl_->open_notify(
      info.cn_info, info.num_elems, nm, [&](int fd) { process_message_notification(fd); }, quiet,
      timeout_connect_sec);
  if (res != SUCCESS) {
    if (!quiet) {
      vlog_error(VCAT_NODE, "Observer notify: failure to open the topic %s: %s", topic_name_.c_str(),
                 NodeErrorToStr(res));
    }
  }

  msg_type_ = info.msg_name;
  msg_cbuftxt_ = info.msg_cbuftxt;
  msg_hash_ = info.msg_hash;
  return res;
}

bool observer::process_message_notification(int fd) {
  (void)fd;
  bool message_processed = false;
  NodeError res;
  // By doing this call on the callback we ensure that for shared memory
  // subscribers the callback will return the most recent message
  // Network subscribers do this on the get_message part already
  if (this->impl_ && this->impl_->is_open()) {
    while (this->impl_->get_num_available() > 0) {
      MsgBufPtr msg = this->get_message(res, 0);
      if (res == node::SUCCESS) {
        message_callback_(msg);
        message_processed = true;
      } else if (res == node::NO_MESSAGE_AVAILABLE) {
        vlog_warning(VCAT_NODE, "Topic %s received a notification but saw no new message",
                     topic_name_.c_str());
        break;
      } else {
        vlog_error(VCAT_NODE, "Topic %s had error %s trying to get a message on notification",
                   topic_name_.c_str(), NodeErrorToStr(res));
        break;
      }
    }
  }
  return message_processed;
}

rpcsubs_impl::~rpcsubs_impl() {
  quit_thread_ = true;
  if (th_.joinable()) {
    th_.join();
  }
}

NodeError rpcsubs_impl::open(const channel_info& cn_info, u32 num, bool quiet, float timeout_sec) {
  (void)num;
  (void)quiet;
  channel_ip_ = cn_info.channel_ip;
  port_ = cn_info.channel_port;
  timeout_sec_ = timeout_sec;

  quit_thread_ = false;
  VLOG_ASSERT(!th_.joinable());
  th_ = std::thread([this]() { thread_function(); });

  return node::SUCCESS;
}

NodeError rpcsubs_impl::open_notify([[maybe_unused]] const channel_info& cn_info,
                                    [[maybe_unused]] u32 num_elems, [[maybe_unused]] NotificationManager& nm,
                                    [[maybe_unused]] NotificationManager::NotifyCallback cb,
                                    [[maybe_unused]] bool quiet, [[maybe_unused]] float timeout_sec) {
  // Function makes no sense for rpc subs, used in observer
  return NodeError::GENERIC_ERROR;
}

void rpcsubs_impl::thread_function() {
  while (!quit_thread_) {
    if (requester_.is_open() || (requester_.open(timeout_sec_, 0.05f, channel_ip_, port_) == SUCCESS)) {
      auto req_pub = requester_.total_published_messages();
      if (req_pub > 0) {
        // Only do this when we have success, a positive number of messages
        num_published_ = req_pub;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

// this function will open the socket and fill the information if successful
NodeError sink_base::open(topic_info* info) {
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);

  receiving_buffer_.resize(1024 * 64);    // Max 64 Kbyte of UDP message
  transmitting_buffer_.resize(4 * 1024);  // Small for internal replies

  // Creating socket file descriptor
  if ((listen_socket_ = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0)) < 0) {
    vlog_error(VCAT_NODE, "Socket creation for sink failed with errno %d: %s", errno, strerror(errno));
    return PUBLISHER_SOCKET_COULD_NOT_BE_CREATED;
  }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = 0;

  if (bind(listen_socket_, (struct sockaddr*)&address, sizeof(address)) < 0) {
    perror("bind failed");
    return PUBLISHER_SOCKET_BIND_ERROR;
  }

  if (getsockname(listen_socket_, (struct sockaddr*)&address, &addrlen) < 0) {
    perror("getsockname failed");
  }

  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  if (setsockopt(listen_socket_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv) < 0) {
    vlog_error(VCAT_NODE, "Error on Sink socket topic %s setting timeout: %s", topic_name_.c_str(),
               strerror(errno));
    return PUBLISHER_SOCKET_FCNTL_ERROR;
  }

  port_ = ntohs(address.sin_port);
  host_ip_ = get_host_ips();

  info->cn_info.channel_ip = host_ip_;
  info->cn_info.is_network = true;
  info->cn_info.channel_port = port_;

  topic_name_ = info->topic_name;
  listener_ = std::thread([this]() { this->thread_function(); });

  vlog_info(VCAT_NODE, "Opened Sink on %s:%d", host_ip_.c_str(), port_);
  return SUCCESS;
}

void sink_base::close() {
  quit_thread_ = true;
  if (listener_.joinable()) listener_.join();
  if (listen_socket_ != -1) {
    ::close(listen_socket_);
    listen_socket_ = -1;
  }
}

void sink_base::handle_incoming_message(ssize_t bytes_read, struct sockaddr* addr, u32 addrlen) {
  bool internal_request = false;
  cbuf_preamble* pre;
  pre = (cbuf_preamble*)receiving_buffer_.data();

  if (pre->hash == rpc_internal_request::TYPE_HASH) {
    internal_request = true;
  }

  size_t transmitting_size = 0;
  u8* bytes = nullptr;
  rpc_internal_reply internal_reply;
  NodeError res;

  if (internal_request) {
    // Handle internal request
    rpc_internal_request intreq;
    if (!intreq.decode_net((char*)receiving_buffer_.data(), (int)bytes_read)) {
      internal_reply.return_code = NodeError::IDL_DECODE_ERROR;
    } else {
      internal_reply.return_code = NodeError::SUCCESS;
      if (intreq.type == RpcInternalRequestType::NUM_REQUESTS_HANDLED) {
        internal_reply.num_requests_handled = num_msg_received_;
      } else if (intreq.type == RpcInternalRequestType::TERMINATE_RPC) {
        internal_reply.return_code = NodeError::GENERIC_ERROR;
      } else {
        internal_reply.return_code = NodeError::GENERIC_ERROR;
      }
    }
    transmitting_size = internal_reply.encode_size();
    bytes = (u8*)&internal_reply;

    ssize_t sent_bytes = sendto(listen_socket_, bytes, transmitting_size, 0, addr, addrlen);
    if (sent_bytes < 0) {
      vlog_error(VCAT_NODE, "Sink on topic %s error while doing an internal reply: %s", topic_name_.c_str(),
                 strerror(errno));
    }
  } else {
    // We got a valid message, do the callback work
    res = handleMsg();

    // Right now, no reply back for normal messages

    // Now send the reply back
    if (res != SUCCESS) {
      vlog_error(VCAT_NODE, "Sink on topic %s error while processing message: %s", topic_name_.c_str(),
                 NodeErrorToStr(res));
    }

    // do not count internal request in the number handled
    num_msg_received_++;
  }
}

void sink_base::thread_function() {
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);
  snprintf(thread_name_, sizeof(thread_name_), "sink_%s", topic_name_.c_str());
  pthread_setname_np(pthread_self(), thread_name_);

  while (!quit_thread_) {
    ssize_t bytes_read;
    bytes_read = recvfrom(listen_socket_, receiving_buffer_.data(), receiving_buffer_.size(), 0,
                          (struct sockaddr*)&address, &addrlen);
    if (bytes_read < 0) {
      if (errno == ETIMEDOUT) {
        // timed out, loop again
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        vlog_debug(VCAT_NODE, "Sink topic %s Got an EAGAIN, continuing", topic_name_.c_str());
        continue;
      }

      vlog_error(VCAT_NODE, "Sink topic %s got error: %s", topic_name_.c_str(), strerror(errno));
      continue;
    }

    VLOG_ASSERT(bytes_read < static_cast<long int>(receiving_buffer_.size()),
                "Any incoming message must fit in our buffer");
    handle_incoming_message(bytes_read, (struct sockaddr*)&address, addrlen);
  }
  vlog_info(VCAT_NODE, "Thread for Sink on topic %s terminating...", topic_name_.c_str());
}

NodeError source_base::open(const std::string& channel_ip, uint16_t port) {
  channel_ip_ = channel_ip;
  channel_port_ = port;

  VLOG_ASSERT(sockfd_ == -1);
  if (host_ips_[0][0] == 0) {
    get_host_ips_vec(host_ips_);
  }
  char ipvec[5][20] = {};
  static_split(channel_ip, ';', ipvec);

  // First, let's see if we have an IP on the same subnet
  std::string ip_same_subnet;
  for (auto& hostip : host_ips_) {
    if (hostip[0] == 0) break;
    for (auto& clientip : ipvec) {
      if (clientip[0] == 0) break;
      if (same_subnet(hostip, clientip)) {
        ip_same_subnet = clientip;
        break;
      }
    }
    if (!ip_same_subnet.empty()) {
      break;
    }
  }

  if (ip_same_subnet.empty()) {
    if (!skip_logging_) {
      vlog_error(VCAT_NODE, "For Sink/source (udp in general), we need the same network! Channel ip: %s",
                 channel_ip.c_str());
    }
    return PRODUCER_NOT_PRESENT;
  }

  server_address_ = (struct sockaddr_in*)malloc(sizeof(struct sockaddr_in));
  VLOG_ASSERT(server_address_ != nullptr);

  memset(server_address_, 0, sizeof(struct sockaddr_in));

  server_address_->sin_family = AF_INET;
  server_address_->sin_port = htons(port);

  if ((sockfd_ = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0)) < 0) {
    if (!skip_logging_) {
      vlog_error(VCAT_NODE, "Source on topic %s Could not create socket: %s\n", topic_name_.c_str(),
                 strerror(errno));
    }
    return SUBSCRIBER_SOCKET_ERROR;
  }

  if (inet_pton(AF_INET, ip_same_subnet.c_str(), &server_address_->sin_addr) <= 0) {
    if (!skip_logging_) {
      vlog_error(VCAT_NODE, "Source on topic %s inet_pton error: %s\n", topic_name_.c_str(), strerror(errno));
    }
    return SUBSCRIBER_SOCKET_ERROR;
  }

  connected_ip_ = ip_same_subnet;

  return SUCCESS;
}

NodeError source_base::sendMessageFromBuffer() {
  size_t transmitting_size = transmitting_buffer_.size();
  u8* bytes = transmitting_buffer_.data();
  rpc_internal_reply internal_reply;

  if (sockfd_ == -1) {
    // we got disconnect, hammer again
    // now this only tries socket reconnect, no whole node reconnect
    NodeError res = source_base::open(channel_ip_, channel_port_);
    if (res != SUCCESS) {
      return res;
    }
  }

  ssize_t sent_bytes = 0;
  sent_bytes = sendto(sockfd_, bytes, transmitting_size, 0, (struct sockaddr*)server_address_,
                      sizeof(struct sockaddr_in));
  if (sent_bytes < 0) {
    if (!skip_logging_) {
      vlog_warning(VCAT_NODE, "Source on topic %s, node %s could not send %zu bytes: %s", topic_name_.c_str(),
                   node_name_.c_str(), transmitting_size, strerror(errno));
    }
    return SOCKET_WRITE_ERROR;
  } else if (sent_bytes != static_cast<long int>(transmitting_size)) {
    if (!skip_logging_) {
      vlog_warning(VCAT_NODE, "Source on topic %s, node %s could only send %zd bytes but tried %zu",
                   topic_name_.c_str(), node_name_.c_str(), sent_bytes, transmitting_size);
    }
    return SOCKET_WRITE_ERROR;
  }
  return SUCCESS;
}

u32 source_base::get_num_messages() {
  rpc_internal_request req;
  req.type = RpcInternalRequestType::NUM_REQUESTS_HANDLED;
  size_t sz = req.encode_net_size();
  transmitting_buffer_.resize(sz);
  auto bres = req.encode_net((char*)transmitting_buffer_.data(), (u32)sz);
  if (!bres) {
    vlog_error(VCAT_NODE, "Source Failure to encode internal request!");
    return 0;
  }

  auto res = sendMessageFromBuffer();
  if (res != SUCCESS) {
    vlog_error(VCAT_NODE, "Source Failure to send internal request: %s", NodeErrorToStr(res));
    return 0;
  }

  rpc_internal_reply reply;
  socklen_t slen = sizeof(sockaddr_in);
  ssize_t bytes_read = recvfrom(sockfd_, &reply, sizeof(reply), 0, (struct sockaddr*)server_address_, &slen);
  if (bytes_read != sizeof(reply)) {
    vlog_error(VCAT_NODE,
               "Source Error receiving reply for internal request, read %zd bytes but expected %zu",
               bytes_read, sizeof(reply));
    return 0;
  }
  return reply.num_requests_handled;
}

void source_base::close() {
  if (sockfd_ != -1) ::close(sockfd_);
  sockfd_ = -1;
  connected_ip_.clear();
  if (server_address_ != nullptr) {
    free(server_address_);
    server_address_ = nullptr;
  }
}

sourcesubs_impl::~sourcesubs_impl() {
  quit_thread_ = true;
  if (th_.joinable()) {
    th_.join();
  }
}

NodeError sourcesubs_impl::open(const channel_info& cn_info, u32 num, bool quiet, float timeout_sec) {
  (void)num;
  (void)quiet;
  channel_ip_ = cn_info.channel_ip;
  port_ = cn_info.channel_port;

  quit_thread_ = false;
  VLOG_ASSERT(!th_.joinable());
  th_ = std::thread([this]() { thread_function(); });

  return node::SUCCESS;
}

NodeError sourcesubs_impl::open_notify([[maybe_unused]] const channel_info& cn_info,
                                       [[maybe_unused]] u32 num_elems,
                                       [[maybe_unused]] NotificationManager& nm,
                                       [[maybe_unused]] NotificationManager::NotifyCallback cb,
                                       [[maybe_unused]] bool quiet, [[maybe_unused]] float timeout_sec) {
  // Function makes no sense for source subs, used in observer

  return NodeError::GENERIC_ERROR;
}

void sourcesubs_impl::thread_function() {
  while (!quit_thread_) {
    if (source_.is_open() || (source_.open(channel_ip_, port_) == SUCCESS)) {
      auto num = source_.get_num_messages();
      if (num > 0) {
        // Only do this when we have success, a positive number of messages
        num_published_ = num;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

}  // namespace node
