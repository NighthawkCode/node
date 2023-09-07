#pragma once

#include <unistd.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "node/nodeerr.h"
#include "node/nodelib.h"
#include "node/types.h"
// This dependency with cbufparser is needed for hash mismatch
// my only gripe is that it can pull in hjson, but that could be
// fixed by subclassing cbufparser
#include "CBufParser.h"
#include "vlog.h"

class circular_buffer;
struct message_bookkeep;

namespace node {

void* helper_open_channel(const channel_info& info, int& mem_fd);
void helper_clean(void* addr, int mem_fd, u32 mem_length);
void helper_clean(int socket);

static inline float fabs(float a) { return a < 0 ? -a : a; }
static inline bool feq(float a, float b) { return fabs(a - b) < 0.000001f; }

template <class T>
class subscriber;

// Simple smart pointer for messages.  Subset of functionality of
// unique_ptr.  This allows NodeApps to hold onto a message outside their
// message handler.

template <class Msg>
class Subscription;

template <typename Msg>
class MsgPtr {
  friend class SubscriptionBase;
  friend class Subscription<Msg>;

  // Construct a new MsgPtr and transfer ownership
  MsgPtr(MsgPtr&& rhs) {
    sub_ = rhs.sub_;
    ptr_ = rhs.ptr_;
    msg_index_ = rhs.msg_index_;

    rhs.sub_ = nullptr;
    rhs.ptr_ = nullptr;
    rhs.msg_index_ = 0;
  }

  // Assignment operator - transfers ownership from RHS.  Use like this:
  // new_ptr = std::move(old_ptr);
  void operator=(MsgPtr&& rhs) {
    if (ptr_ != nullptr) {
      if (ptr_ == rhs.ptr_) {
        assert(sub_ == rhs.sub_);
        rhs.ptr_ = nullptr;
        rhs.sub_ = nullptr;
        rhs.msg_index_ = 0;
        return;
      }
      release();
    }
    sub_ = rhs.sub_;
    ptr_ = rhs.ptr_;
    msg_index_ = rhs.msg_index_;

    rhs.sub_ = nullptr;
    rhs.ptr_ = nullptr;
    rhs.msg_index_ = 0;
  }

public:
  // Empty constructor.
  MsgPtr() {}

  // Destructor.  Deallocates message.
  ~MsgPtr() { release(); }

  // Get the index
  unsigned int getIdx() const { return msg_index_; }

  // Accessor.
  Msg* get() { return ptr_; }

  // Dereference operator.
  Msg* operator->() const {
    assert(ptr_ != nullptr);
    return ptr_;
  }

  Msg& operator*() const {
    assert(ptr_ != nullptr);
    return *ptr_;
  }

  // Convenience operator
  operator bool() const { return ptr_ != nullptr; }

  // Releases ownership and deallocates message.
  void release();

  // This function should only be called from inside the subscriber class
  void make_empty() {
    ptr_ = nullptr;
    sub_ = nullptr;
  }

  // Protected constructor to use by friendly Subscription class.
  MsgPtr(Msg* m, node::subscriber<Msg>* sub, unsigned int idx)
      : sub_(sub)
      , ptr_(m)
      , msg_index_(idx) {}

  node::subscriber<Msg>* sub() { return sub_; }

private:
  node::subscriber<Msg>* sub_ = nullptr;
  Msg* ptr_ = nullptr;
  unsigned int msg_index_;
};

class observer;

// Simple smart pointer for messages of unknown type. Subset of functionality of
// unique_ptr.  This allows Observer to return opaque memory for consumption
class MsgBufPtr {
public:
  // Empty constructor.
  MsgBufPtr() {}

  // Construct a new MsgPtr and transfer ownership
  MsgBufPtr(MsgBufPtr&& rhs) {
    obs_ = rhs.obs_;
    ptr_ = rhs.ptr_;
    msg_index_ = rhs.msg_index_;

    rhs.obs_ = nullptr;
    rhs.ptr_ = nullptr;
    rhs.msg_index_ = 0;
  }

  // Destructor.  Deallocates message.
  ~MsgBufPtr() { release(); }

  // Get the index
  unsigned int getIdx() const { return msg_index_; }

  // Accessor.
  u8* getMem() { return ptr_; }
  size_t getSize() { return size_; }

  // Convenience operator
  operator bool() const { return ptr_ != nullptr; }

  // Assignment operator - transfers ownership from RHS.  Use like this:
  // new_ptr = std::move(old_ptr);
  void operator=(MsgBufPtr&& rhs) {
    if (ptr_ != nullptr) {
      if (ptr_ == rhs.ptr_) {
        assert(obs_ == rhs.obs_);
        rhs.ptr_ = nullptr;
        rhs.obs_ = nullptr;
        rhs.msg_index_ = 0;
        rhs.size_ = 0;
        return;
      }
      release();
    }
    obs_ = rhs.obs_;
    ptr_ = rhs.ptr_;
    size_ = rhs.size_;
    msg_index_ = rhs.msg_index_;

    rhs.obs_ = nullptr;
    rhs.ptr_ = nullptr;
    rhs.size_ = 0;
    rhs.msg_index_ = 0;
  }

  // Releases ownership and deallocates message.
  void release();

  // This function should only be called from inside the observer class
  void make_empty() {
    ptr_ = nullptr;
    size_ = 0;
    obs_ = nullptr;
  }

  MsgBufPtr(u8* m, size_t sz, node::observer* obs, unsigned int idx)
      : obs_(obs)
      , ptr_(m)
      , size_(sz)
      , msg_index_(idx) {}

  node::observer* obs() { return obs_; }

private:
  node::observer* obs_ = nullptr;
  u8* ptr_ = nullptr;
  size_t size_ = 0;
  unsigned int msg_index_;
};

// This class allows applications that subscribe
// to receive a callback, notification, when there
// is a new message published
class NotificationManager {
public:
  using NotifyCallback = std::function<void(int)>;

private:
  int epoll_fd_ = -1;      // FD for epoll
  int inotify_fd_ = -1;    // FD for inotify
  char buf_[4096];         // local buffer for inotify
  std::string node_name_;  // Useful for debugging

  // This map holds both socketfd callbacks and inotify callbacks
  std::unordered_map<uint64_t, NotifyCallback> notification_cbs_;
  std::unordered_map<uint64_t, std::string> fd_to_names_;

  bool setup_epoll();
  bool setup_inotify();
  void handle_inotify_events();

  void remove_notification(int32_t fd, int32_t wd);

public:
  NotificationManager() = default;
  ~NotificationManager();

  void set_node_name(const std::string& node_name) { node_name_ = node_name; }
  void set_node_name(const char* node_name) { node_name_ = node_name; }
  bool add_socket_notification(int sockfd, NotifyCallback cb, const std::string& topic_name);
  int32_t add_shm_notification(const std::string& fname, NotifyCallback cb, const std::string& topic_name);

  void remove_socket_notification(int32_t fd);
  void remove_shm_notification(int32_t wd);
  // Return values:
  // -1 - error
  // 0 - timeout, nothing done
  // 1 - handled and dispatched callbacks
  int wait_for_notification(int timeout_ms = 1000);
};

class publisher_base {
protected:
  std::string topic_name;
  std::string message_name;  // For introspection
  uint64_t message_hash;     // For introspection
  uint8_t variant = 0;
  bool network = false;

public:
  virtual ~publisher_base() {}

  void set_topic_name(const std::string& name) { topic_name = name; }

  void set_topic_variant(uint8_t var) { variant = var; }

  void set_message_name(const std::string& name) { message_name = name; }

  void set_message_hash(const uint64_t hash) { message_hash = hash; }

  void set_network(bool n) { network = n; }

  const std::string get_message_name() const { return message_name; }

  const std::string get_topic_name() const { return topic_name; }

  uint64_t get_message_hash() const { return message_hash; }

  uint8_t get_topic_variant() const { return variant; }

  virtual size_t get_num_published() const = 0;

  // This function only makes sense in non-network publishers
  // returns the size of a message being published
  virtual size_t get_message_size() const = 0;

  // Returns the number of messages on the publishing queue
  virtual u32 get_num_messages() const = 0;

  virtual const char* get_message_cbuf_def() const = 0;

  bool get_network() const { return network; }

  virtual bool enable_checksum() = 0;

  // Call this function to start the effort to end publishing
  virtual void preclose() = 0;
};

class publisher_impl {
public:
  virtual NodeError open(const topic_info* info) = 0;
  virtual bool transmit_message(const u8* bytes, size_t size) = 0;
  virtual bool is_open() const = 0;
  virtual void fill_channel_info(topic_info& info) = 0;
  virtual void close() = 0;
  virtual u8* get_memory_for_message() { return nullptr; }
  virtual u64 get_num_published() const = 0;
  virtual u32 get_num_elements() const = 0;

  virtual bool is_network() const { return false; }
  virtual bool enable_checksum() = 0;
  virtual ~publisher_impl() {}
  virtual void preclose() = 0;
};

class publisher_impl_network : public publisher_impl {
  std::set<int> socket_clients;
  int listen_socket = -1;
  uint16_t port = -1;
  std::string topic_name_;  // for thread naming
  char thread_name[16];
  u64 num_published = 0;
  std::string host_ip;
  std::thread listener;
  std::mutex mtx;
  bool accept_new_clients = true;
  bool use_checksum_ = false;
  int timeout_ms_ = 50;  // A default of 50ms timeout for transmitting a message

  void thread_function();
  bool send_bytes_internal(const u8* bytes, size_t size, int sk);

  publisher_impl_network(const publisher_impl_network& other) = delete;
  publisher_impl_network& operator=(const publisher_impl_network& other) = delete;
  publisher_impl_network(publisher_impl_network&& rhs) = delete;
  publisher_impl_network& operator=(publisher_impl_network&& rhs) = delete;

public:
  publisher_impl_network() = default;

  ~publisher_impl_network() override { close(); }

  NodeError open(const topic_info* info) override;
  bool transmit_message(const u8* bytes, size_t size) override;
  bool is_open() const override { return listen_socket != -1; }
  void fill_channel_info(topic_info& info) override;
  void close() override;
  bool is_network() const override { return true; }
  u64 get_num_published() const override { return num_published; }
  u32 get_num_elements() const override { return 0; }
  bool enable_checksum() override {
    use_checksum_ = true;
    return true;
  }

  uint16_t get_port() const { return port; }
  const std::string& get_host_ip() const { return host_ip; }
  void preclose() override { accept_new_clients = false; }
};

class publisher_impl_shm : public publisher_impl {
  circular_buffer* indices = nullptr;  // Indices should be allocated inside of data...
  message_bookkeep* bk = nullptr;      // Also allocated inside of data
  u8* data = nullptr;
  int mem_fd = -1;
  int notify_fd = -1;
  u32 mem_length = 0;
  u8* elems = nullptr;
  u32 aligned_elem_size = 0;
  u32 num_elems = 0;

  // Do not allow for copy/move operator or constructor
  publisher_impl_shm(publisher_impl_shm const& other) = delete;
  publisher_impl_shm& operator=(publisher_impl_shm const& other) = delete;
  publisher_impl_shm(publisher_impl_shm&& other) = delete;
  publisher_impl_shm& operator=(publisher_impl_shm&& other) = delete;

public:
  publisher_impl_shm(u32 num_elems, u32 elem_size);
  bool is_open() const override { return mem_fd != -1; }
  void fill_channel_info(topic_info& info) override;
  NodeError open(const topic_info* info) override;
  u8* get_memory_for_message() override;
  bool transmit_message(const u8* data, size_t sz) override;
  void close() override;
  u64 get_num_published() const override;
  u32 get_num_elements() const override { return num_elems; }
  bool enable_checksum() override { return false; }
  void preclose() override {}

  ~publisher_impl_shm() override;
};

template <class T>
class publisher : public publisher_base {
private:
  T* msg = nullptr;
  std::vector<u8> buffer;
  publisher_impl* impl = nullptr;

  // Do not allow for copy operator or constructor
  publisher(publisher<T> const& other) = delete;
  publisher& operator=(publisher<T> const& other) = delete;

public:
  publisher() = default;
  publisher(const std::string& topic) { topic_name = topic; }

  publisher(publisher<T>&& rhs) {
    this->topic_name = std::move(rhs.topic_name);
    this->message_name = std::move(rhs.message_name);
    this->message_hash = rhs.message_hash;
    this->variant = rhs.variant;
    std::swap(impl, rhs.impl);
    std::swap(msg, rhs.msg);
    std::swap(buffer, rhs.buffer);
  }

  publisher& operator=(publisher<T>&& rhs) {
    this->topic_name = std::move(rhs.topic_name);
    this->message_name = std::move(rhs.message_name);
    this->message_hash = rhs.message_hash;
    this->variant = rhs.variant;
    std::swap(impl, rhs.impl);
    std::swap(msg, rhs.msg);
    std::swap(buffer, rhs.buffer);

    return *this;
  }

  bool is_open() const { return impl && impl->is_open(); }

  size_t get_message_size() const override {
    if (network) return 0;
    return sizeof(T);
  }

  const char* get_message_cbuf_def() const override { return T::cbuf_string; }

  // this function will check with master, open the socket
  NodeError open(int num_elems = 10, bool is_network = false, int timeout_ms = 50) {
    NodeError res;
    // Find the registry, inquire about this channel
    topic_info info;

    res = check_master();
    if (res != SUCCESS) {
      return res;
    }

    if (!is_network && !T::is_simple()) {
      return CBUF_MSG_NOT_SUPPORTED;
    }

    if (num_elems < 1) {
      return INVALID_PARAMETERS;
    }

    info.topic_name = this->topic_name;
    info.msg_name = T::TYPE_STRING;
    info.msg_hash = T::TYPE_HASH;
    info.msg_cbuftxt = T::cbuf_string;
    info.num_elems = num_elems;
    info.type = node::TopicType::PUB_SUB;
    // fill the timeout for the channel
    info.cn_info.timeout_ms = timeout_ms;

    network = is_network;

    if (is_network) {
      msg = new T;
      impl = new publisher_impl_network();
      res = impl->open(&info);
      if (res != SUCCESS) {
        return res;
      }

      impl->fill_channel_info(info);
      info.visible = true;
      res = create_topic(info);
      if (res != SUCCESS) {
        delete impl;
        impl = nullptr;
        return res;
      }
      return res;
    }

    // Shared memory case
    impl = new publisher_impl_shm(num_elems, sizeof(T));
    info.visible = false;

    impl->fill_channel_info(info);

    res = create_topic(info);
    if (res != SUCCESS) {
      return res;
    }

    res = impl->open(&info);
    if (res != SUCCESS) {
      return res;
    }

    res = make_topic_visible(this->topic_name);
    return res;
  }

  // Producer: get a pointer to a struct to fill
  T* prepare_message() {
    if (impl->is_network()) {
      return msg;
    } else {
      // This call might block
      T* e = new (impl->get_memory_for_message()) T;
      return e;
    }
  }

  // Producer: This function assumes that the image* previously returned will no longer be used
  bool transmit_message(T* elem) {
    if (!impl->is_network()) {
      return impl->transmit_message(nullptr, 0);
    }

    if (T::is_simple() && !T::supports_compact()) {
      return impl->transmit_message((const u8*)elem->encode(), elem->encode_size());
    } else {
      auto size = elem->encode_net_size();
      if (buffer.size() < size) {
        buffer.resize(size);
      }
      elem->encode_net((char*)buffer.data(), size);
      return impl->transmit_message(buffer.data(), size);
    }
  }

  u64 get_num_published() const override { return impl->get_num_published(); }

  u32 get_num_messages() const override { return impl->get_num_elements(); }

  bool enable_checksum() override { return impl->enable_checksum(); }

  void preclose() override {
    if (impl != nullptr) impl->preclose();
  }

  ~publisher() override {
    if (impl) {
      delete impl;
      impl = nullptr;
    }
    if (msg) {
      delete msg;
      msg = nullptr;
    }
  }
};

// Publisher generic is a publisher that is not type
// aware. There is still a type, only that the publisher
// does not know it, and will simply pass through bytes
// instead of a class.
class publisher_generic : public publisher_base {
private:
  std::vector<u8> buffer;
  publisher_impl* impl = nullptr;
  std::string cbuf_string_;
  u64 msg_size_ = 0;
  u32 num_elements_ = 0;
  bool allow_hash_mismatch_ = false;

  // Do not allow for copy operator or constructor
  publisher_generic(publisher_generic const& other) = delete;
  publisher_generic& operator=(publisher_generic const& other) = delete;

public:
  publisher_generic() = default;
  publisher_generic(const std::string& topic) { topic_name = topic; }

  // We want our own Move constructors and assignments because the
  // impl member needs to be swapped, same as the buffer. By default
  // the compiler would generate a move which is not correct.
  publisher_generic(publisher_generic&& rhs) {
    this->topic_name = std::move(rhs.topic_name);
    this->message_name = std::move(rhs.message_name);
    this->message_hash = rhs.message_hash;
    this->variant = rhs.variant;
    this->msg_size_ = rhs.msg_size_;
    this->allow_hash_mismatch_ = rhs.allow_hash_mismatch_;
    std::swap(impl, rhs.impl);
    this->buffer = std::move(rhs.buffer);
  }

  publisher_generic& operator=(publisher_generic&& rhs) {
    this->topic_name = std::move(rhs.topic_name);
    this->message_name = std::move(rhs.message_name);
    this->message_hash = rhs.message_hash;
    this->variant = rhs.variant;
    this->msg_size_ = rhs.msg_size_;
    this->allow_hash_mismatch_ = rhs.allow_hash_mismatch_;
    std::swap(impl, rhs.impl);
    this->buffer = std::move(rhs.buffer);

    return *this;
  }

  bool is_open() const { return impl && impl->is_open(); }

  size_t get_message_size() const override { return msg_size_; }

  u32 get_num_messages() const override { return num_elements_; }

  void allow_hash_mismatch() { allow_hash_mismatch_ = true; }

  // this function will check with master, open the socket
  NodeError open(u64 hash, const char* type_string, const char* cbuf_string, bool is_simple, u64 msg_size = 0,
                 u32 num_elems = 10, bool is_network = false) {
    NodeError res;
    // Find the registry, inquire about this channel
    topic_info info;

    res = check_master();
    if (res != SUCCESS) {
      return res;
    }

    if (!is_network && !is_simple) {
      return CBUF_MSG_NOT_SUPPORTED;
    }

    if (num_elems < 3) {
      return INVALID_PARAMETERS;
    }

    num_elements_ = num_elems;

    info.topic_name = this->topic_name;
    info.msg_name = type_string;
    info.msg_hash = hash;
    info.msg_cbuftxt = cbuf_string;
    info.num_elems = num_elems;
    info.allow_hash_mismatch = allow_hash_mismatch_;
    info.type = node::TopicType::PUB_SUB;
    info.cn_info.timeout_ms = 30;  // Set a default timeout

    network = is_network;
    cbuf_string_ = cbuf_string;

    if (is_network) {
      impl = new publisher_impl_network();
      res = impl->open(&info);
      if (res != SUCCESS) {
        return res;
      }

      impl->fill_channel_info(info);
      info.visible = true;
      res = create_topic(info);
      if (res != SUCCESS) {
        delete impl;
        impl = nullptr;
        return res;
      }
      return res;
    }

    if (msg_size == 0) {
      // This should really be invalid parameters or such
      return INVALID_PARAMETERS;
    }
    // Shared memory case
    impl = new publisher_impl_shm(num_elems, msg_size);
    info.visible = false;

    impl->fill_channel_info(info);

    res = create_topic(info);
    if (res != SUCCESS) {
      return res;
    }

    res = impl->open(&info);
    if (res != SUCCESS) {
      return res;
    }

    res = make_topic_visible(this->topic_name);
    return res;
  }

  const char* get_message_cbuf_def() const override { return cbuf_string_.c_str(); }

  // this function will transmit the message without checking the bytes sent
  // actually form a meaningful message. Only on generic publisher
  void transmit_message(const u8* bytes, size_t byte_size) {
    if (!impl->is_network()) {
      memcpy(impl->get_memory_for_message(), bytes, byte_size);
    }
    impl->transmit_message(bytes, byte_size);
  }

  u64 get_num_published() const override { return impl->get_num_published(); }

  bool enable_checksum() override { return impl->enable_checksum(); }

  void preclose() override {
    if (impl != nullptr) impl->preclose();
  }

  ~publisher_generic() override {
    if (impl) {
      delete impl;
      impl = nullptr;
    }
  }
};

// Constants for subscriber::open()
constexpr float NODE_WAIT_FOREVER = -1.0f;       // Retry indefinitely
constexpr float NODE_MINIMAL_WAIT = 0.01f;       // minimal wait for normal networks
constexpr float NODE_NO_RETRY = 0.0f;            // Fail if producer not ready
constexpr float NODE_DEFAULT_RETRY_SECS = 2.0f;  // Default retry duration
constexpr float NODE_DEFAULT_WAIT_TIMEOUT = 1.0f;

class subscribe_impl {
protected:
  NotificationManager* notification_manager_ = nullptr;

public:
  virtual void set_topic_name(const std::string& topic_name) = 0;
  virtual void set_node_name(const std::string& name) = 0;
  virtual bool is_open() const = 0;
  virtual bool is_alive() const = 0;
  // setting a timeout_sec to be minimal in this interface, implementations can override it to something more
  // reasonable for that implementation
  virtual NodeError open(const channel_info& cn_info, u32 num_elems, bool quiet = true,
                         float timeout_sec = NODE_MINIMAL_WAIT) = 0;
  virtual NodeError open_notify(const channel_info& cn_info, u32 num_elems, NotificationManager& nm,
                                NotificationManager::NotifyCallback cb, bool quiet = false,
                                float timeout_sec = NODE_MINIMAL_WAIT) = 0;
  virtual bool is_there_new() = 0;
  virtual void reset_message_index() = 0;
  virtual s32 get_last_index() const = 0;
  virtual u8* get_message(NodeError& result, float timeout_sec, size_t* msg_size = nullptr) = 0;
  virtual void release_message(u32 idx) = 0;
  virtual bool is_network() { return false; }
  virtual size_t get_num_published() const = 0;
  virtual size_t get_num_available() const = 0;
  virtual size_t get_max_available() const = 0;
  virtual void always_consume_message() {}
  virtual void debug_string(std::string& dbg) { (void)dbg; }
  virtual void preclose() = 0;
  virtual ~subscribe_impl() {}
};

class subscribe_impl_shm : public subscribe_impl {
private:
  circular_buffer* indices = nullptr;  // Indices are allocated inside of data
  message_bookkeep* bk = nullptr;      // Also a pointer inside data, shared memory
  u8* data = nullptr;
  int mem_fd = -1;
  u32 mem_length = 0;
  u8* elems = nullptr;
  u32 num_elems = 0;
  u32 aligned_elem_size = 0;
  s32 last_index = -1;
  std::string topic_name_;
  std::string node_name_;
  s32 wd = -1;

  subscribe_impl_shm(subscribe_impl_shm const& other) = delete;
  subscribe_impl_shm& operator=(subscribe_impl_shm const& other) = delete;
  subscribe_impl_shm(subscribe_impl_shm&& rhs) = delete;
  subscribe_impl_shm& operator=(subscribe_impl_shm&& rhs) = delete;

  void close();

public:
  subscribe_impl_shm(u32 sz)
      : aligned_elem_size(sz) {}

  ~subscribe_impl_shm() override { close(); }

  void set_topic_name(const std::string& topic_name) override { topic_name_ = topic_name; }
  void set_node_name(const std::string& name) override { node_name_ = name; }
  bool is_open() const override { return mem_fd >= 0; }
  // Extend this to check on the memory if the publisher is alive
  bool is_alive() const override { return is_open(); }

  // shared memory susbscribers should be opened quickly, currently we don't use
  // this parameter but we have to supply it as it is part of the interface
  NodeError open(const channel_info& cn_info, u32 num, bool quiet = true,
                 float timeout_sec = NODE_MINIMAL_WAIT) override;
  NodeError open_notify(const channel_info& cn_info, u32 num_elems, NotificationManager& nm,
                        NotificationManager::NotifyCallback cb, bool quiet = false,
                        float timeout_sec = NODE_MINIMAL_WAIT) override;

  bool is_there_new() override;

  void debug_string(std::string& dbg) override;

  void reset_message_index() override { last_index = -1; }

  u64 get_num_published() const override;

  s32 get_last_index() const override { return last_index; }

  u8* get_message(NodeError& result, float timeout_sec, size_t* msg_size = nullptr) override;

  void release_message(u32 idx) override;

  size_t get_num_available() const override;

  size_t get_max_available() const override { return num_elems; }
  void preclose() override {}
};

class subscribe_impl_network : public subscribe_impl {
private:
  int sockfd_ = -1;
  int pipefd_[2] = {-1, -1};
  u32 num_elems;
  u64 num_published = 0;
  mutable std::mutex mtx;
  std::condition_variable cond_v;
  std::string topic_name_;
  std::string node_name_;
  char thread_name_[16] = {};
  char host_ips_[20][20] = {};
  std::thread th;
  std::queue<std::vector<u8>> received_messages;
  std::queue<std::vector<u8>> available_buffers;
  std::vector<u8> checkedout_buffer;
  bool quit_thread = false;
  bool allow_empty_queue = false;

  subscribe_impl_network(subscribe_impl_network const& other) = delete;
  subscribe_impl_network& operator=(subscribe_impl_network const& other) = delete;
  subscribe_impl_network(subscribe_impl_network&& rhs) = delete;
  subscribe_impl_network& operator=(subscribe_impl_network&& rhs) = delete;

  bool read_preamble_from_socket(cbuf_preamble& pre);
  bool read_cbuf_from_socket(u8* data, size_t sz_to_read);
  bool read_message_from_socket();
  void thread_function();
  NodeError open_internal(const channel_info& cn_info, u32 num, bool quiet,
                          float timeout_sec = NODE_DEFAULT_WAIT_TIMEOUT);

  void close();

public:
  subscribe_impl_network() {}
  ~subscribe_impl_network() override;

  void set_topic_name(const std::string& topic_name) override { topic_name_ = topic_name; }
  void set_node_name(const std::string& name) override { node_name_ = name; }
  bool is_open() const override { return sockfd_ >= 0; }
  bool is_alive() const override { return !quit_thread && is_open(); }
  s32 get_last_index() const override { return 0; }
  bool is_network() override { return true; }

  // We wan't to give more time for network connections so the timeout_sec is a bigger default here
  NodeError open(const channel_info& cn_info, u32 num, bool quiet = true,
                 float timeout_sec = NODE_DEFAULT_WAIT_TIMEOUT) override;
  NodeError open_notify(const channel_info& cn_info, u32 num_elems, NotificationManager& nm,
                        NotificationManager::NotifyCallback cb, bool quiet = false,
                        float timeout_sec = NODE_DEFAULT_WAIT_TIMEOUT) override;
  bool is_there_new() override;
  void reset_message_index() override;
  u8* get_message(NodeError& result, float timeout_sec, size_t* msg_size = nullptr) override;
  NodeError get_all_messages(std::function<void(u8*, size_t)> fn);
  void release_message(u32 idx) override;
  size_t get_num_published() const override { return num_published; }
  size_t get_num_available() const override;
  size_t get_max_available() const override { return num_elems; }
  void always_consume_message() override { allow_empty_queue = true; }
  void preclose() override { quit_thread = true; }
};

template <class T>
class subscriber {
private:
  subscribe_impl* impl = nullptr;
  u32 pid = 0;
  T* local_msg = nullptr;
  std::function<void(node::MsgPtr<T>&)> message_callback_;
  CBufParser* src_parser_ = nullptr;
  CBufParser* dst_parser_ = nullptr;

  std::string topic_name_;
  std::string node_name_;
  bool receive_latest_message_ = true;
  bool requires_msg_conversion_ = false;
  bool allow_compact_conversion_ = false;  // For replaying compact messages on SHM topics

  NodeError open_internal(topic_info& info, float timeout_sec = NODE_DEFAULT_WAIT_TIMEOUT,
                          float retry_delay_sec = NODE_DEFAULT_RETRY_SECS, const std::string& nmip = "") {
    NodeError res;
    // Find the registry, inquire about this channel

    assert(retry_delay_sec >= 0.0 && retry_delay_sec <= 60.0);

    res = get_topic_info(topic_name_, info, nmip);
    if (res == SOCKET_SERVER_NOT_FOUND) {
      if (feq(timeout_sec, NODE_WAIT_FOREVER)) {
        vlog_error(VCAT_NODE, "Failure to find the node master when subscribing for topic %s",
                   topic_name_.c_str());
      }
      return res;
    }
    if (res == TOPIC_NOT_FOUND && !feq(timeout_sec, NODE_NO_RETRY)) {
      float remaining_sec = timeout_sec;
      // printf("Topic %s not found.  Retrying.", topic_name.c_str());
      while (res == TOPIC_NOT_FOUND && (feq(timeout_sec, NODE_WAIT_FOREVER) || remaining_sec > 0)) {
        // printf(".");
        // fflush(stdout);
        usleep(static_cast<useconds_t>(retry_delay_sec * 1e6f));
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
    allow_compact_conversion_ = info.allow_hash_mismatch;
    bool alloc_parser = false;
    if (T::supports_compact() && allow_compact_conversion_ && !info.cn_info.is_network) {
      alloc_parser = true;
    }
    if (info.msg_hash != T::TYPE_HASH || alloc_parser) {
      if (!info.allow_hash_mismatch) return HASH_MISMATCH;
      if (info.msg_name != T::TYPE_STRING) {
        // If the names do not match, do not allow it, likely very bad
        return HASH_MISMATCH;
      }
      requires_msg_conversion_ = info.msg_hash != T::TYPE_HASH;
      src_parser_ = new CBufParser();
      dst_parser_ = new CBufParser();
      if (!src_parser_->ParseMetadata(info.msg_cbuftxt, info.msg_name)) {
        return IDL_DECODE_ERROR;
      }
      if (!dst_parser_->ParseMetadata(T::cbuf_string, T::TYPE_STRING)) {
        return IDL_DECODE_ERROR;
      }
    }
    if (info.type != node::TopicType::PUB_SUB) {
      return TOPIC_TYPE_MISMATCH;
    }

    if (info.cn_info.is_network) {
      impl = new subscribe_impl_network();
    } else {
      // In the shared memory case,
      if (requires_msg_conversion_) {
        // When we require conversion, pass 0
        // as the size so it will use the publisher's
        impl = new subscribe_impl_shm(0);
      } else {
        // we want to specify our type's size to ensure matching
        // with the publisher's size
        impl = new subscribe_impl_shm((sizeof(T) + 15) & ~0x0F);
      }
    }
    VLOG_ASSERT(!node_name_.empty());
    impl->set_topic_name(topic_name_);
    impl->set_node_name(node_name_);

    return res;
  }

public:
  subscriber() = default;

  // Do not allow for {copy, move} operator or constructor
  subscriber(subscriber<T> const& other) = delete;
  subscriber& operator=(subscriber<T> const& other) = delete;
  subscriber(subscriber<T>&& rhs) = delete;
  subscriber& operator=(subscriber<T>&& rhs) = delete;

  void set_topic_name(const std::string_view name) { topic_name_ = std::string{name}; }
  void set_node_name(const std::string_view name) { node_name_ = std::string{name}; }
  void set_topic_name(const std::string& name) { topic_name_ = name; }
  void set_node_name(const std::string& name) { node_name_ = name; }
  void set_topic_name(const char* name) { topic_name_ = name; }
  void set_node_name(const char* name) { node_name_ = name; }
  // This flag controls if, when notified that we have incoming messages and we are using
  // NotificationManager, if there will be a callback to the latest message or a loop
  // over all the new messages.
  void set_receive_next_message() { receive_latest_message_ = false; }

  const std::string& topic_name() const { return topic_name_; }

  bool is_open() const { return impl && impl->is_open(); }
  bool is_alive() const { return impl && impl->is_alive(); }

  void debug_string(std::string& dbg) {
    if (impl) impl->debug_string(dbg);
  }

  bool process_message_notification(int fd) {
    (void)fd;
    bool message_processed = false;
    NodeError res;
    // By doing this call on the callback we ensure that for shared memory
    // subscribers the callback will return the most recent message
    // Network subscribers do this on the get_message part already
    if (this->impl && this->impl->is_open()) {
      size_t num_messages_to_process = 0;
      if (this->receive_latest_message_) {
        num_messages_to_process = 1;
        if (is_network()) {
          this->impl->reset_message_index();
        } else if (this->impl->is_there_new()) {
          this->impl->reset_message_index();
        } else {
          // This can happen when we are on a loop processing messages on this callback, and a message
          // arrives then, the callback then processes the latest message, but there is a notification still
          // on the NM that gets handled later.
          vlog_finest(VCAT_NODE, "Topic %s had no new messages on shm, returning!!!", topic_name_.c_str());
          return message_processed;
        }
      } else {
        num_messages_to_process = this->impl->get_num_available();
      }

      for (int num_processed = 0; num_processed < num_messages_to_process; num_processed++) {
        MsgPtr<T> msg = this->get_message(res, 0);
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
  // when using open_notify, if there are no calls to NotificationManager timely enough
  // to service all incoming messages, the most recent message is the one in the
  // callback
  NodeError open_notify(NotificationManager& nm, std::function<void(node::MsgPtr<T>&)> cb,
                        float timeout_nm_sec = NODE_MINIMAL_WAIT,
                        float timeout_connect_sec = NODE_MINIMAL_WAIT, float retry_delay_sec = NODE_NO_RETRY,
                        bool quiet = true) {
    NodeError res;
    // Find the registry, inquire about this channel
    topic_info info;

    res = open_internal(info, timeout_nm_sec, retry_delay_sec, "");
    if (res != SUCCESS) {
      return res;
    }

    message_callback_ = cb;

    res = impl->open_notify(
        info.cn_info, info.num_elems, nm, [this](int fd) { this->process_message_notification(fd); },
        timeout_connect_sec);

    if (res != SUCCESS) {
      if (!quiet) {
        vlog_error(VCAT_NODE, "Subscriber notify: failure to open the topic %s: %s", topic_name_.c_str(),
                   NodeErrorToStr(res));
      }
    }

    pid = (u32)getpid();

    return res;
  }

  // this function will open the channel, allocate memory, set the indices, and
  // do any other needed initialization
  // Args:
  //   timeout_nm_sec: The timeout used while connecting to the nodemaster.
  //   timeout_connect_sec: The timeout used while connecting to the actual publisher.
  //             This is mainly useful when the publisher is a network publisher. It is not
  //             currently used with shared memory systems
  //   retry_delay_sec: The retry delay used before trying to reconnect with the nodemaster
  //   nmip: The hostname to find the nodemaster, by default it will try to find a nodemaster on localhost
  //   quiet: If set to true no log statements will be printed to the console
  NodeError open(float timeout_nm_sec = NODE_DEFAULT_WAIT_TIMEOUT,
                 float timeout_connect_sec = NODE_DEFAULT_WAIT_TIMEOUT, float retry_delay_sec = NODE_NO_RETRY,
                 const std::string& nmip = "", bool quiet = true) {
    NodeError res;
    // Find the registry, inquire about this channel
    topic_info info;

    // try querying the registry
    res = open_internal(info, timeout_nm_sec, retry_delay_sec, nmip);
    if (res != SUCCESS) {
      return res;
    }

    // try connecting to that channel
    res = impl->open(info.cn_info, info.num_elems, quiet, timeout_connect_sec);
    if (res != SUCCESS) {
      if (!quiet) {
        vlog_error(VCAT_NODE, "Subscriber failure to open the topic %s : %s", topic_name_.c_str(),
                   NodeErrorToStr(res));
      }
    }

    pid = (u32)getpid();

    // printf("Opened channel %s with %d length, %s path\n", topic_name_.c_str(),
    //        mem_length, info.cn_info.channel_path.c_str());

    // printf("Subscribed to topic %s\n", topic_name_.c_str());

    return res;
  }

  /// Non blocking call to see if there is new data
  bool is_there_new() { return impl && impl->is_there_new() && impl->is_open(); }

  void reset_message_index() {
    if (impl) impl->reset_message_index();
  }

  size_t get_num_published() const {
    if (impl) return impl->get_num_published();
    return 0;
  }

  // return how many available messages are there
  // This can serve as a measure of back pressure
  size_t get_num_available() const {
    if (impl && impl->is_open()) return impl->get_num_available();
    return 0;
  }

  // Get the total number of messages after which
  // reuse does happen
  size_t get_max_available() const {
    if (impl) return impl->get_max_available();
    return 0;
  }

  ///
  /// \brief get_message
  /// \param result is an error value
  /// \return The message itself
  ///
  /// This function will obtain a message for the consumer.
  /// If <last_index> is -1, it will obtain the most recently published message
  /// If <last_index> is not -1, it will try to obtain the message at last_index+1
  /// Call `reset_message_index` to always obtain the most recent one
  ///
  MsgPtr<T> get_message(NodeError& result, float timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC) {
    if (impl && impl->is_open()) {
      size_t msg_size = 0;
      u8* ptr = impl->get_message(result, timeout_sec, &msg_size);

      if (result != SUCCESS) return {};
      bool msg_conversion_compact = false;
      if (allow_compact_conversion_ && !impl->is_network() && T::supports_compact()) {
        // In replayer cases, for SHM and compact messages there is a real chance
        // that the message from the publisher (replayer) is compacted.
        // Force conversion to uncompact it
        msg_conversion_compact = true;
        cbuf_preamble* pre = (cbuf_preamble*)ptr;
        msg_size = pre->size();
      }

      if (requires_msg_conversion_ || msg_conversion_compact) {
        if (local_msg == nullptr) {
          local_msg = new T;
        }
        int res = src_parser_->FastConversion(T::TYPE_STRING, ptr, msg_size, *dst_parser_, T::TYPE_STRING,
                                              (unsigned char*)local_msg, sizeof(T));
        if (res == 0) {
          result = IDL_DECODE_ERROR;
          impl->release_message(impl->get_last_index());
          return {};
        }
        return MsgPtr<T>(local_msg, this, impl->get_last_index());
      }

      if (impl->is_network()) {
        if (T::is_simple() && !T::supports_compact()) {
          return MsgPtr<T>((T*)ptr, this, impl->get_last_index());
        }
        if (local_msg == nullptr) {
          local_msg = new T;
        }
        local_msg->Init();
        VLOG_ASSERT(local_msg != nullptr);
        bool res = local_msg->decode_net((char*)ptr, msg_size);
        if (res) {
          result = SUCCESS;
          return MsgPtr<T>(local_msg, this, impl->get_last_index());
        } else {
          result = IDL_DECODE_ERROR;
          impl->release_message(impl->get_last_index());
          return {};
        }
      } else {
        if (result == SUCCESS) {
          return MsgPtr<T>((T*)ptr, this, impl->get_last_index());
        }
      }
    }

    result = PRODUCER_NOT_PRESENT;
    return MsgPtr<T>(nullptr, nullptr, 0);
  }

  NodeError get_all_messages(std::vector<T>& vec) {
    if (!impl || !impl->is_network()) return GENERIC_ERROR;
    subscribe_impl_network* nimpl = (subscribe_impl_network*)impl;
    vec.clear();
    nimpl->get_all_messages([&](u8* ptr, size_t sz) {
      vec.emplace_back();
      bool res = vec.back().decode_net((char*)ptr, sz);
      if (!res) {
        vlog_error(VCAT_NODE, "Could not decode one buffer on the get all messages, discarding it");
        vec.pop_back();
      }
    });
    return SUCCESS;
  }

  // Consumer: This function assumes that the image* previously returned will no longer be used
  void release_message(MsgPtr<T>& elem) {
    impl->release_message(elem.getIdx());
    elem.make_empty();
  }

  bool is_network() const { return impl && impl->is_network(); }

  void consume_message_once() {
    if (impl) impl->always_consume_message();
  }

  void close() {
    if (impl) {
      delete impl;
      impl = nullptr;
    }
    if (local_msg) {
      delete local_msg;
      local_msg = nullptr;
    }
    if (src_parser_) {
      delete src_parser_;
      src_parser_ = nullptr;
    }
    if (dst_parser_) {
      delete dst_parser_;
      dst_parser_ = nullptr;
    }
  }

  void preclose() {
    if (impl) impl->preclose();
  }
  ~subscriber() { close(); }
};

template <class T>
void MsgPtr<T>::release() {
  if (ptr_ != nullptr) {
    assert(sub_ != nullptr);
    sub_->release_message(*this);
  }
  ptr_ = nullptr;
  sub_ = nullptr;
}

class observer {
protected:
  subscribe_impl* impl = nullptr;

  std::string topic_name_;
  std::string node_name_;
  std::string msg_type_;
  std::string msg_cbuftxt_;
  u64 msg_hash_ = 0;

public:
  observer() = default;

  observer(observer&& rhs)
      : topic_name_(std::move(rhs.topic_name_))
      , node_name_(std::move(rhs.node_name_))
      , msg_type_(std::move(rhs.msg_type_))
      , msg_cbuftxt_(std::move(rhs.msg_cbuftxt_)) {
    std::swap(impl, rhs.impl);
    msg_hash_ = rhs.msg_hash_;
  }

  observer& operator=(observer&& rhs) {
    topic_name_ = std::move(rhs.topic_name_);
    node_name_ = std::move(rhs.node_name_);
    msg_type_ = std::move(rhs.msg_type_);
    msg_cbuftxt_ = std::move(rhs.msg_cbuftxt_);
    std::swap(impl, rhs.impl);
    msg_hash_ = rhs.msg_hash_;
    return *this;
  }

  void set_topic_name(const std::string& name) {
    topic_name_ = name;
    if (impl) impl->set_topic_name(name);
  }

  const std::string_view topic_name() const { return topic_name_; }
  const std::string& msg_type() const { return msg_type_; }
  const std::string& msg_cbuftxt() const { return msg_cbuftxt_; }
  u64 msg_hash() const { return msg_hash_; }

  void set_node_name(const std::string& name) {
    node_name_ = name;
    if (impl) impl->set_node_name(name);
  }
  const std::string_view node_name() const { return node_name_; }

  // this function will open the channel, allocate memory, set the indices, and
  // do any other needed initialization.  if 'quiet' is false, then error messages will be printed
  // on any failure.
  NodeError open(float timeout_sec = NODE_MINIMAL_WAIT, float retry_delay_sec = NODE_DEFAULT_RETRY_SECS,
                 bool quiet = true);

  // when using open_notify, if there are no calls to NotificationManager timely enough
  // to service all incoming messages, the most recent message is the one in the
  // callback
  NodeError open_notify(NotificationManager& nm, std::function<void(node::MsgBufPtr&)> cb,
                        float timeout_nm_sec = NODE_MINIMAL_WAIT,
                        float timeout_connect_sec = NODE_MINIMAL_WAIT, float retry_delay_sec = NODE_NO_RETRY,
                        bool quiet = true);

  size_t get_num_published() const { return impl->get_num_published(); }

  size_t get_max_available() const { return impl->get_max_available(); }

  void reset_message_index() {
    if (impl) impl->reset_message_index();
  }

  /// Non blocking call to see if there is new data
  bool is_there_new() { return impl && impl->is_there_new() && impl->is_open(); }

  ///
  /// \brief get_message
  /// \param result is an error value
  /// \return The message itself
  ///
  /// This function will obtain a message for the consumer/observer.
  /// If <last_index> is -1, it will obtain the most recently published message
  /// If <last_index> is not -1, it will try to obtain the message at last_index+1
  /// Call `reset_message_index` to always obtain the most recent one
  ///
  MsgBufPtr get_message(NodeError& result, float timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC) {
    if (impl && impl->is_open()) {
      size_t msg_size = 0;
      u8* ptr = impl->get_message(result, timeout_sec, &msg_size);

      if (result != SUCCESS) return {};

      return MsgBufPtr(ptr, msg_size, this, impl->get_last_index());
    }

    result = PRODUCER_NOT_PRESENT;
    return {};
  }

  void preclose() {
    if (impl) impl->preclose();
  }

  void release_message(MsgBufPtr& msg);

  ~observer() {
    if (impl) {
      delete impl;
      impl = nullptr;
    }
  }

protected:
  std::function<void(node::MsgBufPtr&)> message_callback_;

  bool process_message_notification(int fd);
};

class response_provider_base {
protected:
  std::string topic_name;
  rpc_info rp_info;
  std::vector<u8> receiving_buffer;
  std::vector<u8> transmitting_buffer;
  std::set<int> socket_clients;
  int listen_socket = -1;
  uint16_t port = -1;
  char thread_name[16];
  u64 num_requests_handled = 0;
  std::string host_ip;
  std::thread listener;
  NotificationManager* nm_ = nullptr;
  bool accept_new_clients = false;
  void thread_function();
  // Returns true if the message was handle, false if the socket got closed
  bool handle_incoming_message(int client_fd);
  // this function will open the socket and fill the information if successful
  NodeError open(topic_info* info);
  NodeError open_notify(topic_info* info, NotificationManager& nm);
  void handle_new_connection(int fd);

public:
  response_provider_base() = default;
  virtual ~response_provider_base() { close(); }

  void set_topic_name(const std::string& name) { topic_name = name; }
  void set_rpc_info(const rpc_info& ri) { rp_info = ri; }

  const std::string& get_topic_name() const { return topic_name; }
  const rpc_info& get_rpc_info() const { return rp_info; }
  u64 get_num_requests_handled() const { return num_requests_handled; }
  virtual void close();
  virtual NodeError handleMsg() = 0;
};

// The callback function processes the request, builds a reply, and
// returns true if the query worked or false otherwise
template <class RequestT, class ReplyT>
using RequestCallbackFn = std::function<bool(RequestT*, ReplyT*)>;

template <class RequestT, class ReplyT>
class response_provider : public response_provider_base {
  RequestCallbackFn<RequestT, ReplyT> callback;
  RequestT* req_obj = nullptr;
  ReplyT* rep_obj = nullptr;

public:
  response_provider() = default;
  ~response_provider() override { close(); }
  response_provider(response_provider<RequestT, ReplyT>&& rhs)
      : callback(std::move(rhs.callback)) {
    std::swap(req_obj, rhs.req_obj);
    std::swap(rep_obj, rhs.rep_obj);
  }

  void setCallback(RequestCallbackFn<RequestT, ReplyT> cb) { callback = cb; }
  NodeError open() {
    // Do all the nodemaster register, etc
    NodeError res;
    // Find the registry, inquire about this channel
    topic_info info;
    info.Init();

    res = check_master();
    if (res != SUCCESS) {
      return res;
    }

    if (!callback) {
      return RESPONSE_CALLBACK_NOT_PROVIDED;
    }

    info.topic_name = this->topic_name;
    info.msg_name = RequestT::TYPE_STRING;
    info.msg_hash = RequestT::TYPE_HASH;
    info.num_elems = 0;
    info.type = node::TopicType::RPC;
    info.rp_info.request_type_name = RequestT::TYPE_STRING;
    info.rp_info.request_type_hash = RequestT::TYPE_HASH;
    info.rp_info.request_cbuftxt = RequestT::cbuf_string;
    info.rp_info.reply_type_name = ReplyT::TYPE_STRING;
    info.rp_info.reply_type_hash = ReplyT::TYPE_HASH;
    info.rp_info.reply_cbuftxt = ReplyT::cbuf_string;

    res = response_provider_base::open(&info);
    if (res != SUCCESS) {
      return res;
    }
    info.visible = true;
    res = create_topic(info);
    if (res != SUCCESS) {
      close();
      return res;
    }
    req_obj = new RequestT();
    rep_obj = new ReplyT();
    return res;
  }

  NodeError open_notify(NotificationManager& nm) {
    // Do all the nodemaster register, etc
    NodeError res;
    // Find the registry, inquire about this channel
    topic_info info;
    info.Init();

    res = check_master();
    if (res != SUCCESS) {
      return res;
    }

    if (!callback) {
      return RESPONSE_CALLBACK_NOT_PROVIDED;
    }

    info.topic_name = this->topic_name;
    info.msg_name = RequestT::TYPE_STRING;
    info.msg_hash = RequestT::TYPE_HASH;
    info.num_elems = 0;
    info.type = node::TopicType::RPC;
    info.rp_info.request_type_name = RequestT::TYPE_STRING;
    info.rp_info.request_type_hash = RequestT::TYPE_HASH;
    info.rp_info.reply_type_name = ReplyT::TYPE_STRING;
    info.rp_info.reply_type_hash = ReplyT::TYPE_HASH;

    res = response_provider_base::open_notify(&info, nm);
    if (res != SUCCESS) {
      return res;
    }
    info.visible = true;
    res = create_topic(info);
    if (res != SUCCESS) {
      close();
      return res;
    }
    req_obj = new RequestT();
    rep_obj = new ReplyT();
    return res;
  }

  // This function is called by the handler thread when there is a request
  // in the receiving_buffer, it expects transmitting buffer to be set or return false
  // Maybe extend this to error types
  NodeError handleMsg() override {
    bool res = req_obj->decode_net((char*)receiving_buffer.data(), receiving_buffer.size());
    if (!res) {
      return IDL_DECODE_ERROR;
    }
    // Note, the reply is cleared or initialized, perf optimization?
    rep_obj->Init();
    if (callback(req_obj, rep_obj)) {
      auto size = rep_obj->encode_net_size();
      transmitting_buffer.resize(size);
      res = rep_obj->encode_net((char*)transmitting_buffer.data(), size);
      VLOG_ASSERT(res, "We should always be able to encode");
      return SUCCESS;
    } else {
      return RESPONSE_CALLBACK_FAILURE;
    }
  }

  void close() override {
    // we need to close first as the thread could be using these objects
    response_provider_base::close();
    if (req_obj) delete req_obj;
    if (rep_obj) delete rep_obj;
    req_obj = nullptr;
    rep_obj = nullptr;
  }
};

class requester_base {
protected:
  std::string topic_name_;
  std::string node_name_;
  std::vector<u8> transmitting_buffer;
  std::vector<u8> receiving_buffer;
  std::vector<u8> receiving_buffer_async_;
  char host_ips_[20][20] = {};
  std::string channel_ip_;
  std::string connected_ip_;
  uint16_t channel_port_;
  int sockfd_ = -1;
  float timeout_sec_ = NODE_DEFAULT_WAIT_TIMEOUT;
  float retry_delay_sec_ = NODE_DEFAULT_RETRY_SECS;
  bool expect_reply = true;
  bool close_request_sent_ = false;
  bool using_notification_manager_ = false;
  NodeError sendRequestOnly();
  NodeError receiveReply(bool async = false);

public:
  requester_base() = default;
  requester_base(const requester_base& other) = delete;
  requester_base(requester_base&& other) = default;
  virtual ~requester_base() { close(); }

  void set_topic_name(const std::string& name) { topic_name_ = name; }
  void set_node_name(const std::string& name) { node_name_ = name; }
  const std::string& get_topic_name() const { return topic_name_; }
  const std::string& get_node_name() const { return node_name_; }
  const std::string& get_connected_ip() const { return connected_ip_; }
  virtual void close();
  void close_cleanup();
  virtual void handleAsyncCallback();
  bool is_open() const { return sockfd_ != -1; }
  int total_published_messages();

  // This function opens a socket
  NodeError open(float timeout_sec, float retry_delay_sec, const std::string& channel_ip, uint16_t port);
  NodeError open_notify(float timeout_sec, float retry_delay_sec, const std::string& channel_ip,
                        uint16_t port, NotificationManager& nm);
  NodeError sendRequest();

  static const u32 recv_timeout_ms = 500;  // in Ms
};

template <class RequestT, class ReplyT>
class requester : public requester_base {
  std::function<void(NodeError, const ReplyT& reply)> cb_;
  ReplyT* replyObj = nullptr;

  NodeError open_internal(topic_info& info, float timeout_sec, float retry_delay_sec) {
    NodeError result;
    info.Init();

    VLOG_ASSERT(!topic_name_.empty(), "A topic name is a must for a connection");

    assert(retry_delay_sec >= 0.0 && retry_delay_sec <= 60.0);

    result = get_topic_info(topic_name_, info);
    if (result == SOCKET_SERVER_NOT_FOUND) {
      vlog_error(VCAT_NODE, "Failure to find the node master when subscribing for topic %s",
                 topic_name_.c_str());
      return result;
    }
    if (result == TOPIC_NOT_FOUND && !feq(retry_delay_sec, NODE_NO_RETRY)) {
      float remaining_sec = timeout_sec;
      while (result == TOPIC_NOT_FOUND && (feq(timeout_sec, NODE_WAIT_FOREVER) || remaining_sec > 0)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(u32(retry_delay_sec * 1e3)));
        remaining_sec -= retry_delay_sec;
        result = get_topic_info(topic_name_, info);
      }
    }
    if (result != SUCCESS) {
      // Consumer cannot create new topics
      if (feq(timeout_sec, NODE_WAIT_FOREVER)) {
        // fprintf(stderr, "Failure to find the topic %s in the node registry\n",
        //        topic_name_.c_str());
      }
      if (result == TOPIC_NOT_FOUND) {
        return PRODUCER_NOT_PRESENT;
      }
      return result;
    }

    if (info.type != node::TopicType::RPC) {
      return TOPIC_TYPE_MISMATCH;
    }
    if (info.rp_info.request_type_hash != RequestT::TYPE_HASH ||
        info.rp_info.reply_type_hash != ReplyT::TYPE_HASH) {
      vlog_error(VCAT_NODE,
                 "We found topic %s with Request (%s, %lX) and Reply (%s, %lX) but we needed Request (%s, "
                 "%lX) and Reply (%s, %lX)",
                 topic_name_.c_str(), info.rp_info.request_type_name.c_str(), info.rp_info.request_type_hash,
                 info.rp_info.reply_type_name.c_str(), info.rp_info.reply_type_hash, RequestT::TYPE_STRING,
                 RequestT::TYPE_HASH, ReplyT::TYPE_STRING, ReplyT::TYPE_HASH);
      return HASH_MISMATCH;
    }
    return SUCCESS;
  }

public:
  requester() = default;
  requester(const requester& other) = delete;
  requester(requester&& other) = default;
  ~requester() override {
    close();
    if (replyObj != nullptr) {
      delete replyObj;
      replyObj = nullptr;
    }
  }
  NodeError open(float timeout_sec = NODE_DEFAULT_WAIT_TIMEOUT,
                 float retry_delay_sec = NODE_DEFAULT_RETRY_SECS) {
    NodeError result;
    topic_info info;

    result = open_internal(info, timeout_sec, retry_delay_sec);
    if (result != SUCCESS) return result;

    result = requester_base::open(timeout_sec, retry_delay_sec, info.cn_info.channel_ip,
                                  info.cn_info.channel_port);
    return result;
  }

  NodeError open_notify(NotificationManager& nm, std::function<void(NodeError, const ReplyT& reply)> cb,
                        float timeout_sec = NODE_MINIMAL_WAIT, float retry_delay_sec = NODE_NO_RETRY) {
    NodeError result;
    topic_info info;

    result = open_internal(info, timeout_sec, retry_delay_sec);
    if (result != SUCCESS) return result;

    cb_ = cb;
    result = requester_base::open_notify(timeout_sec, retry_delay_sec, info.cn_info.channel_ip,
                                         info.cn_info.channel_port, nm);
    return result;
  }

  NodeError makeRequest(const RequestT& request, ReplyT& reply) {
    NodeError res;
    size_t encoding_size = request.encode_net_size();
    transmitting_buffer.resize(encoding_size);
    if (!request.encode_net((char*)transmitting_buffer.data(), encoding_size)) {
      return IDL_ENCODE_ERROR;
    }
    res = sendRequest();
    if (res != SUCCESS) {
      return res;
    }
    if (!reply.decode_net((char*)receiving_buffer.data(), receiving_buffer.size())) {
      return IDL_DECODE_ERROR;
    }
    return res;
  }

  NodeError makeRequestAsync(const RequestT& request) {
    NodeError res;
    if (using_notification_manager_ == false) {
      // Open with notification manager is a must for Async requests
      return NodeError::ASYNC_REQUEST_NOT_SUPPORTED;
    }
    size_t encoding_size = request.encode_net_size();
    transmitting_buffer.resize(encoding_size);
    if (!request.encode_net((char*)transmitting_buffer.data(), encoding_size)) {
      return IDL_ENCODE_ERROR;
    }
    res = sendRequestOnly();
    if (res != SUCCESS) {
      return res;
    }
    return res;
  }

  void handleAsyncCallback() override {
    NodeError res;
    if (replyObj == nullptr) {
      replyObj = new ReplyT();
    }
    if (!replyObj->decode_net((char*)receiving_buffer_async_.data(), receiving_buffer_async_.size())) {
      res = IDL_DECODE_ERROR;
    } else {
      res = SUCCESS;
    }
    cb_(res, *replyObj);
  }
};

class rpcsubs_impl : public subscribe_impl {
private:
  u32 num_published = 0;
  requester_base requester_;
  std::string topic_name_;
  std::string node_name_;
  std::string channel_ip_;
  u16 port_;
  std::thread th;
  bool quit_thread = false;
  float timeout_sec_ = NODE_MINIMAL_WAIT;

  rpcsubs_impl(rpcsubs_impl const& other) = delete;
  rpcsubs_impl& operator=(rpcsubs_impl const& other) = delete;
  rpcsubs_impl(rpcsubs_impl&& rhs) = delete;
  rpcsubs_impl& operator=(rpcsubs_impl&& rhs) = delete;

  void thread_function();

public:
  rpcsubs_impl() {}
  ~rpcsubs_impl() override;

  void set_topic_name(const std::string& topic_name) override { requester_.set_topic_name(topic_name); }
  void set_node_name(const std::string& name) override { requester_.set_node_name(name); }
  bool is_open() const override { return requester_.is_open(); }
  bool is_alive() const override { return is_open(); }
  s32 get_last_index() const override { return 0; }
  bool is_network() override { return true; }

  // Setting this to a minimal wait, will be used for setting the timeout for rpc requester connection
  NodeError open(const channel_info& cn_info, u32 num, bool quiet = true,
                 float timeout_sec = NODE_MINIMAL_WAIT) override;
  NodeError open_notify(const channel_info& cn_info, u32 num_elems, NotificationManager& nm,
                        NotificationManager::NotifyCallback cb, bool quiet = false,
                        float timeout_sec = NODE_MINIMAL_WAIT) override;
  bool is_there_new() override { return false; }
  void reset_message_index() override {}
  u8* get_message(NodeError& result, float timeout_sec, size_t* msg_size = nullptr) override {
    (void)result;
    (void)timeout_sec;
    (void)msg_size;
    return nullptr;
  }
  NodeError get_all_messages(std::function<void(u8*, size_t)> fn) {
    (void)fn;
    return SUCCESS;
  }
  void release_message(u32 idx) override { (void)idx; }
  size_t get_num_published() const override { return num_published; }
  size_t get_num_available() const override { return 0; }
  size_t get_max_available() const override { return 0; }
  void always_consume_message() override {}
  void preclose() override { quit_thread = true; }
};

// Sink and source are a way in node to have multiple publishers
// (the source) with a single subscriber (the sink). In this case the sink
// must be started first, the channel is always network communication using
// UDP. The canonical use case for this is distributed / remote logging.

class sink_base {
protected:
  std::string topic_name_;
  std::vector<u8> receiving_buffer_;
  std::vector<u8> transmitting_buffer_;
  int listen_socket_ = -1;
  uint16_t port_ = -1;
  char thread_name_[16];
  u32 num_msg_received_ = 0;
  std::string host_ip_;
  std::thread listener_;
  bool quit_thread_ = false;
  void thread_function();
  void handle_incoming_message(ssize_t bytes_read, struct sockaddr* addr, u32 addrlen);
  // this function will open the socket and fill the information if successful
  NodeError open(topic_info* info);

public:
  sink_base() = default;
  virtual ~sink_base() { close(); }

  void set_topic_name(const std::string& name) { topic_name_ = name; }

  const std::string& get_topic_name() const { return topic_name_; }
  u32 get_num_received() const { return num_msg_received_; }
  virtual void close();
  virtual NodeError handleMsg() = 0;
};

// The callback function processes the incoming message
template <class T>
using SinkCallbackFn = std::function<void(T*)>;

template <class T>
class sink : public sink_base {
  SinkCallbackFn<T> callback_;
  T* msg_obj_ = nullptr;

public:
  sink() = default;
  ~sink() override { close(); }
  sink(sink<T>&& rhs)
      : callback_(rhs.callback_) {
    std::swap(topic_name_, rhs.topic_name_);
    std::swap(receiving_buffer_, rhs.receiving_buffer_);
    std::swap(transmitting_buffer_, rhs.transmitting_buffer_);
    std::swap(listen_socket_, rhs.listen_socket_);
    std::swap(port_, rhs.port_);
    std::swap(host_ip_, rhs.host_ip_);
    std::swap(listener_, rhs.listener_);
    std::swap(msg_obj_, rhs.msg_obj_);
    memcpy(thread_name_, rhs.thread_name_, sizeof(thread_name_));
  }

  void setCallback(SinkCallbackFn<T> cb) { callback_ = cb; }

  NodeError open(SinkCallbackFn<T> cb = {}) {
    // Do all the nodemaster register, etc
    NodeError res;
    // Find the registry, inquire about this channel
    topic_info info;
    info.Init();

    res = check_master();
    if (res != SUCCESS) {
      return res;
    }

    if (cb) {
      setCallback(cb);
    }

    if (!callback_) {
      return RESPONSE_CALLBACK_NOT_PROVIDED;
    }

    info.topic_name = topic_name_;
    info.msg_name = T::TYPE_STRING;
    info.msg_hash = T::TYPE_HASH;
    info.msg_cbuftxt = T::cbuf_string;
    info.num_elems = 0;
    info.type = node::TopicType::SINK_SRC;

    res = sink_base::open(&info);
    if (res != SUCCESS) {
      return res;
    }
    info.visible = true;
    res = create_topic(info);
    if (res != SUCCESS) {
      close();
      return res;
    }
    msg_obj_ = new T();
    return res;
  }

  // This function is called by the handler thread when there is a request
  // in the receiving_buffer, it expects transmitting buffer to be set or return false
  // Maybe extend this to error types
  NodeError handleMsg() override {
    bool res = msg_obj_->decode_net((char*)receiving_buffer_.data(), receiving_buffer_.size());
    if (!res) {
      return IDL_DECODE_ERROR;
    }
    callback_(msg_obj_);
    return SUCCESS;
  }

  void close() override {
    // we need to close first as the thread could be using these objects
    sink_base::close();
    if (msg_obj_) delete msg_obj_;
    msg_obj_ = nullptr;
  }
};

class source_base {
protected:
  std::string topic_name_;
  std::string node_name_;
  std::vector<u8> transmitting_buffer;
  std::string channel_ip_;
  std::string connected_ip_;
  struct sockaddr_in* server_address_ = nullptr;
  char host_ips_[20][20] = {};
  uint16_t channel_port_;
  int sockfd_ = -1;
  bool expect_reply = true;
  bool close_request_sent_ = false;
  bool using_notification_manager_ = false;
  bool skip_logging_ = false;

public:
  source_base() = default;
  source_base(const source_base& other) = delete;
  source_base(source_base&& other) = default;
  virtual ~source_base() { close(); }

  void set_topic_name(const std::string& name) { topic_name_ = name; }
  void set_node_name(const std::string& name) { node_name_ = name; }
  const std::string& get_topic_name() const { return topic_name_; }
  const std::string& get_node_name() const { return node_name_; }
  const std::string& get_connected_ip() const { return connected_ip_; }
  virtual void close();
  bool is_open() const { return sockfd_ != -1; }
  void skip_logging() { skip_logging_ = true; }

  // This function opens a socket
  NodeError open(const std::string& channel_ip, uint16_t port);
  NodeError sendMessageFromBuffer();
  u32 get_num_messages();

  static const u32 recv_timeout_ms = 500;  // in Ms
};

template <class T>
class source : public source_base {
  NodeError open_internal(topic_info& info) {
    NodeError result;
    info.Init();

    VLOG_ASSERT(!topic_name_.empty(), "A topic name is a must for a connection");

    result = get_topic_info(topic_name_, info);
    if (result == SOCKET_SERVER_NOT_FOUND) {
      vlog_error(VCAT_NODE, "Failure to find the node master when subscribing for topic %s",
                 topic_name_.c_str());
      return result;
    }
    if (result != SUCCESS) {
      if (result == TOPIC_NOT_FOUND) {
        return PRODUCER_NOT_PRESENT;
      }
      return result;
    }

    if (info.type != node::TopicType::SINK_SRC) {
      return TOPIC_TYPE_MISMATCH;
    }
    if (info.msg_hash != T::TYPE_HASH) {
      vlog_error(VCAT_NODE, "We found topic %s with message (%s, %lX) but we needed message (%s, %lX)",
                 topic_name_.c_str(), info.msg_name.c_str(), info.msg_hash, T::TYPE_STRING, T::TYPE_HASH);
      return HASH_MISMATCH;
    }
    return SUCCESS;
  }

public:
  source() = default;
  source(const source& other) = delete;
  source(source&& other) = default;
  ~source() override { close(); }
  NodeError open() {
    NodeError result;
    topic_info info;

    result = open_internal(info);
    if (result != SUCCESS) return result;

    result = source_base::open(info.cn_info.channel_ip, info.cn_info.channel_port);
    return result;
  }

  NodeError sendMessage(const T& msg) {
    NodeError res;
    size_t encoding_size = msg.encode_net_size();
    transmitting_buffer.resize(encoding_size);
    if (!msg.encode_net((char*)transmitting_buffer.data(), encoding_size)) {
      return IDL_ENCODE_ERROR;
    }
    res = sendMessageFromBuffer();
    return res;
  }
};

class sourcesubs_impl : public subscribe_impl {
private:
  u32 num_published = 0;
  source_base source_;
  std::string topic_name_;
  std::string node_name_;
  std::string channel_ip_;
  u16 port_;
  std::thread th;
  bool quit_thread = false;

  sourcesubs_impl(sourcesubs_impl const& other) = delete;
  sourcesubs_impl& operator=(sourcesubs_impl const& other) = delete;
  sourcesubs_impl(sourcesubs_impl&& rhs) = delete;
  sourcesubs_impl& operator=(sourcesubs_impl&& rhs) = delete;

  void thread_function();

public:
  sourcesubs_impl() {}
  ~sourcesubs_impl() override;

  void set_topic_name(const std::string& topic_name) override { source_.set_topic_name(topic_name); }
  void set_node_name(const std::string& name) override { source_.set_node_name(name); }
  bool is_open() const override { return source_.is_open(); }
  bool is_alive() const override { return is_open(); }
  s32 get_last_index() const override { return 0; }
  bool is_network() override { return true; }

  NodeError open(const channel_info& cn_info, u32 num, bool quiet = true,
                 float timeout_sec = NODE_DEFAULT_WAIT_TIMEOUT) override;
  NodeError open_notify(const channel_info& cn_info, u32 num_elems, NotificationManager& nm,
                        NotificationManager::NotifyCallback cb, bool quiet = false,
                        float timeout_sec = NODE_DEFAULT_WAIT_TIMEOUT) override;
  bool is_there_new() override { return false; }
  void reset_message_index() override {}
  u8* get_message(NodeError& result, float timeout_sec, size_t* msg_size = nullptr) override {
    (void)result;
    (void)timeout_sec;
    (void)msg_size;
    return nullptr;
  }
  NodeError get_all_messages(std::function<void(u8*, size_t)> fn) {
    (void)fn;
    return SUCCESS;
  }
  void release_message(u32 idx) override { (void)idx; }
  size_t get_num_published() const override { return num_published; }
  size_t get_num_available() const override { return 0; }
  size_t get_max_available() const override { return 0; }
  void always_consume_message() override {}
  void preclose() override { quit_thread = true; }
};

}  // namespace node
