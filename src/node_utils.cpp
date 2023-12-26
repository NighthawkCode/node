#include "node/node_utils.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <inttypes.h>
#include <libexplain/libexplain.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netpacket/packet.h>
#include <poll.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vlog.h>

#include "defer.h"
#include "node/nodedefault.h"
#include "node/types.h"

#ifdef DEBUG_BUILD
// When doing debug, things are slower, be more generous with timeouts)
static int timeout_scale = 32;
#else
static int timeout_scale = 1;
#endif

// clang-format off
static const char* RequestTypeToStr[] = {
  "CREATE_TOPICS",
  "ADVERTISE_TOPICS",
  "NUM_TOPICS",
  "TOPIC_AT_INDEX",
  "TOPIC_BY_NAME",
  "GET_SESSION_NAME",
  "GET_NEW_SESSION_NAME",
  "GET_LOG_PATH",
  "GET_LOG_FOLDER",
  "GET_PEER_LOG_PATH",
  "RESET_LOG_PATH",
  "GET_TOPICS",
  "GET_NETWORK_TOPICS",
  "ANNOUNCE_NETWORK_TOPICS",
  "ANNOUNCE_SESSION_NAME",
  "GET_PEER_LIST",
  "ANNOUNCE_RECONNECT",
  "GET_NODEMASTER_INFO",
  "REQUEST_TERMINATION",
  "GET_VALUE",
  "SET_VALUE",
  "UPDATE_STORE",
  "GET_STORE",
  "UPDATE_PEERS",
  "REQUEST_SHUTDOWN"
};

static const char* RequestStatusToStr[] = {
  "SUCCESS",
  "TOPIC_NOT_FOUND",
  "INDEX_OUT_OF_BOUNDS",
  "REQUEST_INVALID",
  "TOPIC_COULD_NOT_BE_MADE_VISIBLE",
  "GENERIC_ERROR",
  "KEY_NOT_FOUND",
  "UNAUTHORIZED"
};
// clang-format on

#define CASE_RETURN(x)     \
  case node::NodeError::x: \
    return #x

namespace node {

const char* NodeErrorToStr(node::NodeError err) {
  switch (err) {
    CASE_RETURN(SUCCESS);
    CASE_RETURN(GENERIC_ERROR);
    CASE_RETURN(TOPIC_NOT_FOUND);
    CASE_RETURN(OLD_PRODUCER_IS_STILL_ALIVE);
    CASE_RETURN(SOCKET_COULD_NOT_BE_CREATED);
    CASE_RETURN(SOCKET_SERVER_IP_INCORRECT);
    CASE_RETURN(SOCKET_SERVER_NOT_FOUND);
    CASE_RETURN(SOCKET_FCNTL_ERROR);
    CASE_RETURN(IDL_ENCODE_ERROR);
    CASE_RETURN(SOCKET_WRITE_ERROR);
    CASE_RETURN(SOCKET_REPLY_ERROR);
    CASE_RETURN(IDL_DECODE_ERROR);
    CASE_RETURN(INDEX_OUT_OF_BOUNDS);
    CASE_RETURN(SERVER_INCOMPATIBLE);
    CASE_RETURN(SHARED_MEMORY_OPEN_ERROR);
    CASE_RETURN(NOTIFY_FILE_OPEN_ERROR);
    CASE_RETURN(CONSUMER_LIMIT_EXCEEDED);
    CASE_RETURN(PRODUCER_NOT_PRESENT);
    CASE_RETURN(CONSUMER_TIME_OUT);
    CASE_RETURN(CBUF_MSG_NOT_SUPPORTED);
    CASE_RETURN(HASH_MISMATCH);
    CASE_RETURN(PUBLISHER_SOCKET_COULD_NOT_BE_CREATED);
    CASE_RETURN(PUBLISHER_SOCKET_FCNTL_ERROR);
    CASE_RETURN(PUBLISHER_SOCKET_BIND_ERROR);
    CASE_RETURN(PUBLISHER_SOCKET_LISTEN_ERROR);
    CASE_RETURN(SUBSCRIBER_SOCKET_ERROR);
    CASE_RETURN(SUBSCRIBER_SOCKET_FCNTL_ERROR);
    CASE_RETURN(NO_MESSAGE_AVAILABLE);
    CASE_RETURN(TOPIC_TYPE_MISMATCH);
    CASE_RETURN(RESPONSE_CALLBACK_NOT_PROVIDED);
    CASE_RETURN(RESPONSE_CALLBACK_FAILURE);
    CASE_RETURN(SOCKET_READ_ERROR);
    CASE_RETURN(PRODUCER_CBUF_SIZE_MISMATCH);
    CASE_RETURN(ASYNC_REQUEST_NOT_SUPPORTED);
    CASE_RETURN(INVALID_PARAMETERS);
    CASE_RETURN(KEY_NOT_FOUND);
    CASE_RETURN(KEY_UNAUTHORIZED);
    CASE_RETURN(TOPIC_COULD_NOT_BE_MADE_VISIBLE);
  }
  return "";
}

}  // namespace node

const char* NodeRequestToStr(node::RequestType rt) {
  int a = (int)rt;
  VLOG_ASSERT(a <= (sizeof(RequestTypeToStr) / sizeof(RequestTypeToStr[0])) && a >= 0);
  return RequestTypeToStr[a];
}

void printRequest(const node::registry_request& request, int vlog_level) {
  vlog(vlog_level, VCAT_NODE, "Request with data:");
  vlog(vlog_level, VCAT_NODE, "  RequestType: %s", RequestTypeToStr[(int)request.action]);
  vlog(vlog_level, VCAT_NODE, "  Topic index: %d", request.topic_index);
  vlog(vlog_level, VCAT_NODE, "  Topic name: %s", request.topic_name.c_str());
  for (auto& tp : request.topics) {
    vlog(vlog_level, VCAT_NODE, "    Topic name: %s", tp.topic_name.c_str());
    vlog(vlog_level, VCAT_NODE, "    Msg name: %s", tp.msg_name.c_str());
    vlog(vlog_level, VCAT_NODE, "    Msg Hash: 0x%016" PRIX64, tp.msg_hash);
    vlog(vlog_level, VCAT_NODE, "    Pub PID: %d", tp.publisher_pid);
    vlog(vlog_level, VCAT_NODE, "    Visible: %d", tp.visible);
    vlog(vlog_level, VCAT_NODE, "    Is Network: %d", tp.cn_info.is_network);
    vlog(vlog_level, VCAT_NODE, "    Channel path: %s", tp.cn_info.channel_path.c_str());
    vlog(vlog_level, VCAT_NODE, "    Channel size: %u", tp.cn_info.channel_size);
    vlog(vlog_level, VCAT_NODE, "    Channel IP: %s", tp.cn_info.channel_ip.c_str());
    vlog(vlog_level, VCAT_NODE, "    Channel port: %u", tp.cn_info.channel_port);
  }
}

void printReply(const node::registry_reply& reply, int vlog_level) {
  vlog(vlog_level, VCAT_NODE, "  Return status: %s", RequestStatusToStr[(int)reply.status]);
  vlog(vlog_level, VCAT_NODE, "  Reply Text: %s", reply.text_reply.c_str());
  for (auto& tp : reply.topics) {
    vlog(vlog_level, VCAT_NODE, "    Topic name: %s", tp.topic_name.c_str());
    vlog(vlog_level, VCAT_NODE, "    Msg name: %s", tp.msg_name.c_str());
    vlog(vlog_level, VCAT_NODE, "    Msg Hash: 0x%016" PRIX64, tp.msg_hash);
    vlog(vlog_level, VCAT_NODE, "    Pub PID: %d", tp.publisher_pid);
    vlog(vlog_level, VCAT_NODE, "    Visible: %d", tp.visible);
    vlog(vlog_level, VCAT_NODE, "    Is Network: %d", tp.cn_info.is_network);
    vlog(vlog_level, VCAT_NODE, "    Channel path: %s", tp.cn_info.channel_path.c_str());
    vlog(vlog_level, VCAT_NODE, "    Channel size: %u", tp.cn_info.channel_size);
    vlog(vlog_level, VCAT_NODE, "    Channel IP: %s", tp.cn_info.channel_ip.c_str());
    vlog(vlog_level, VCAT_NODE, "    Channel port: %u", tp.cn_info.channel_port);
  }
}

static char* get_thread_name(char (&buf)[16]) {
  pthread_getname_np(pthread_self(), buf, sizeof(buf));
  return buf;
}

std::string get_host_ips() {
  std::string ret;
  in_addr docker_addr = {0};
  bool seen_docker_addr = false;
  struct ifaddrs *ifaddr, *ifa;
  if (getifaddrs(&ifaddr) == -1) {
    vlog_error(VCAT_NODE, "Error on getifaddrs get_host_ips: %d : %s", errno, strerror(errno));
    return ret;
  }

  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr && ifa->ifa_addr->sa_family == AF_INET) {
      if (ifa->ifa_flags & IFF_LOOPBACK) {
        // loopback interfaces we skip
        continue;
      }

      struct sockaddr_in* paddr = (struct sockaddr_in*)ifa->ifa_addr;
      u8 prefix = paddr->sin_addr.s_addr & 0xFF;
      u8 prefix2 = (paddr->sin_addr.s_addr >> 8) & 0xFF;
      if (prefix == 127) {
        // skip localhost
        continue;
      }
      if (prefix == 172 && prefix2 == 17) {
        // skip common IP addresses for docker
        docker_addr = paddr->sin_addr;
        seen_docker_addr = true;
        continue;
      }
      if (!ret.empty()) {
        ret += ";";
      }
      ret += inet_ntoa(paddr->sin_addr);
    }
  }
  freeifaddrs(ifaddr);
  // in the case we do not have any other valid address, use the docker one
  if (ret.empty() && seen_docker_addr) {
    ret = inet_ntoa(docker_addr);
  }
  return ret;
}

/*
 * Input: array of strings, with a nullptr
 * on the last string. Each string is 20 chars long
 */

bool get_host_ips_vec(char ips[20][20]) {
  std::vector<std::string> ret;
  in_addr docker_addr = {0};
  bool seen_docker_addr = false;
  struct ifaddrs *ifaddr, *ifa;
  if (getifaddrs(&ifaddr) == -1) {
    vlog_error(VCAT_NODE, "Error on getifaddrs get_host_ips_vec: %d : %s", errno, strerror(errno));
    return false;
  }

  int idx = 0;

  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr && ifa->ifa_addr->sa_family == AF_INET) {
      if (ifa->ifa_flags & IFF_LOOPBACK) {
        // loopback interfaces we skip
        continue;
      }
      struct sockaddr_in* paddr = (struct sockaddr_in*)ifa->ifa_addr;
      u8 prefix = paddr->sin_addr.s_addr & 0xFF;
      u8 prefix2 = (paddr->sin_addr.s_addr >> 8) & 0xFF;
      if (prefix == 127) {
        // skip localhost
        continue;
      }
      if (prefix == 172 && prefix2 == 17) {
        // skip common IP addresses for docker
        continue;
      }
      if (idx >= 20) {
        return false;
      }
      strncpy(ips[idx], inet_ntoa(paddr->sin_addr), 20);
      idx++;
    }
  }
  // in the case we do not have any other valid address, use the docker one
  if (idx == 0 && seen_docker_addr) {
    strncpy(ips[idx], inet_ntoa(docker_addr), 20);
    idx++;
  }
  while (idx < 20) {
    ips[idx][0] = 0;
    idx++;
  }
  freeifaddrs(ifaddr);
  return true;
}

static struct timeval SetTimeval(int timeout_ms) {
  int timeout_s = timeout_ms / 1000;
  int remainder = timeout_ms - timeout_s * 1000;
  struct timeval tv;
  tv.tv_sec = timeout_s;
  tv.tv_usec = remainder * 1000;
  return tv;
}

const char* copy_until(char* dst, const char* src, char delim, size_t sz) {
  int copied = 0;
  while (*src != 0 && *src != delim && copied < sz) {
    *dst = *src;
    copied++;
    dst++;
    src++;
  }
  *dst = 0;
  return src;
}

bool same_subnet(const char* ip1, const char* ip2) {
  char ip1_part0[4], ip1_part1[4], ip2_part0[4], ip2_part1[4];
  const char* ip1ptr = copy_until(ip1_part0, ip1, '.', sizeof(ip1_part0));
  copy_until(ip1_part1, ip1ptr + 1, '.', sizeof(ip1_part1));

  const char* ip2ptr = copy_until(ip2_part0, ip2, '.', sizeof(ip2_part0));
  copy_until(ip2_part1, ip2ptr + 1, '.', sizeof(ip2_part1));

  if (strncmp(ip1_part0, ip2_part0, sizeof(ip1_part0))) {
    return false;
  }

  if (strncmp(ip1_part1, ip2_part1, sizeof(ip1_part1))) {
    return false;
  }
  return true;
}

bool read_cbuf(int sockfd, std::vector<char>& buffer, ssize_t& bytes, int timeout) {
  cbuf_preamble pre = {};
  double start_time = time_now();
  bytes = 0;
  while (true) {
    bytes = recv(sockfd, &pre, sizeof(pre), MSG_PEEK);
    timeout *= timeout_scale;
    if (bytes < 0) {
      vlog_debug(VCAT_NODE, "[NodeUtils] time_now() - start_time: %f and timeout: %f",
                 time_now() - start_time, double(timeout) / 1000.0);
      if (time_now() - start_time > double(timeout) / 1000.0) {
        vlog_debug(VCAT_NODE, "Timedout on reading a cbuf, errno: %s", strerror(errno));
        return false;
      }
      if (errno == EAGAIN || errno == EINTR) {
        continue;
      }
      vlog_warning(VCAT_NODE, "Reading cbuf failed on recv with errno %d", errno);
      return false;
    }
    break;
  }

  // ensure we read a cbuf preamble at least
  if (bytes != sizeof(pre)) {
    vlog_debug(VCAT_NODE, "Reading cbuf, read %zd bytes but expected a pre of %zu", bytes, sizeof(pre));
    return false;
  }

  if (pre.size() > buffer.size()) {
    buffer.resize(pre.size());
  }

  bytes = recv(sockfd, buffer.data(), pre.size(), MSG_WAITALL);
  if (bytes != pre.size()) {
    // We should never have EAGAIN since we use WAITALL
    //      if (errno == EAGAIN || errno == EWOULDBLOCK) {
    //        result = NO_MESSAGE_AVAILABLE;
    //        return nullptr;
    //      }
    // we had a bad error. TODO: handle this better
    vlog_debug(VCAT_NODE, "Reading cbuf, received %zd bytes but expected %u", bytes, pre.size());
    return false;
  }

  return true;
}

node::NodeError connect_with_timeout(const std::string& server_ip, uint16_t port, int& sockfd, int timeout_ms,
                                     bool blocking_socket) {
  int res;
  struct sockaddr_in serv_addr;

  sockfd = -1;

  VLOG_ASSERT(!server_ip.empty(), "We cannot have an empty server IP!");

  memset(&serv_addr, 0, sizeof(serv_addr));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port);

  if ((sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)) < 0) {
    vlog_error(VCAT_NODE, "Error : Could not create socket: %s", strerror(errno));
    return node::NodeError::SOCKET_COULD_NOT_BE_CREATED;
  }

  if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) < 0) {
    vlog_error(VCAT_NODE, "Server ip: %s", server_ip.c_str());
    vlog_error(VCAT_NODE, "inet_pton error occured: %s", strerror(errno));
    return node::NodeError::SOCKET_SERVER_IP_INCORRECT;
  }

  // since our socket is non blocking this will return immediately and we can control
  // the timeout threshold
  res = connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
  if (res < 0) {
    // check for usual in-progress kind of error codes which would have
    // ocurred from a non-blocking connect call
    if (errno == EINPROGRESS || errno == EAGAIN || errno == EINTR) {
      // wait on a poll to see when we can "write" on this socket to determine
      // the connection is open and proper. specify the required timout threshold
      pollfd pfd;
      pfd.fd = sockfd;
      pfd.events = POLLOUT;

      int poll_status = poll(&pfd, 1, timeout_ms);

      if (poll_status == 0) {
        vlog_fine(VCAT_NODE, "Node registry server timed out / was not there at %s:%d", server_ip.c_str(),
                  port);
        close(sockfd);
        sockfd = -1;
        return node::NodeError::SOCKET_SERVER_NOT_FOUND;
      } else if (poll_status < 0) {
        vlog_error(VCAT_NODE, "Error on polling when waiting for a connection to nodemaster: %s",
                   strerror(errno));
        close(sockfd);
        sockfd = -1;
        return node::NodeError::SOCKET_SERVER_NOT_FOUND;
      } else {
        if (pfd.revents & POLLHUP) {
          vlog_fine(VCAT_NODE, "Node registry server timed out (revents: %X) / was not there at %s:%d",
                    pfd.revents, server_ip.c_str(), port);
          close(sockfd);
          sockfd = -1;
          return node::NodeError::SOCKET_SERVER_NOT_FOUND;
        }

        if (pfd.revents != POLLOUT) {
          if (pfd.revents == POLLERR) {
            int err;
            socklen_t len = sizeof(err);
            int rc = getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len);
            if (rc < 0) {
              vlog_error(VCAT_NODE,
                         "Poll on connection to nodemaster, poll event: POLLERR. SO_Error failed with RC: "
                         "%d, errno: %d",
                         rc, errno);
            } else if (err == 111) {
              vlog_debug(
                  VCAT_NODE,
                  "Poll on connection to nodemaster errored with POLLERR and error 111, likely high load");
            } else {
              vlog_warning(VCAT_NODE, "Poll on connection to nodemaster errored with POLLERR and error %d",
                           err);
            }
          } else {
            vlog_error(VCAT_NODE, "Poll on connection to nodemaster, unexpected poll event: %d.",
                       pfd.revents);
          }

          close(sockfd);
          sockfd = -1;
          return node::NodeError::SOCKET_SERVER_NOT_FOUND;
        }

        int so_error;
        socklen_t len = sizeof(so_error);

        getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &so_error, &len);

        if (so_error != 0) {
          vlog_warning(
              VCAT_NODE,
              "Node registry server error (%d) on getting socket value from select. Nodemaster at %s:%d",
              so_error, server_ip.c_str(), port);
          close(sockfd);
          sockfd = -1;
          return node::NodeError::SOCKET_SERVER_NOT_FOUND;
        }
      }
    } else {
      vlog_error(VCAT_NODE, "Error on connecting to nodemaster: %d, %s", errno, strerror(errno));
      close(sockfd);
      sockfd = -1;
      return node::NodeError::SOCKET_SERVER_NOT_FOUND;
    }
  }

  if (blocking_socket) {
    // Set the socket blocking again
    if (fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL, 0) ^ O_NONBLOCK) < 0) {
      perror("calling fcntl");
      close(sockfd);
      sockfd = -1;
      return node::NodeError::SOCKET_FCNTL_ERROR;
    }
  }

  int opt = 1;
  int iret = setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (void*)&opt, sizeof(opt));
  if (iret < 0) {
    perror("Setting TCP_NODELAY");
  }

  return node::NodeError::SUCCESS;
}

static node::NodeError send_request_impl(const std::string& server_ip, uint16_t port,
                                         node::registry_request& request, node::registry_reply& reply,
                                         int timeout_ms) {
  int sockfd = -1;
  ssize_t n = 0;
  std::vector<char> recvBuffer(BUFFER_SIZE);

  DEFER(if (sockfd > -1) close(sockfd));

#if DEBUG
  printRequest(request);
#endif

  memset(recvBuffer.data(), 0, BUFFER_SIZE);

  auto result = connect_with_timeout(server_ip, port, sockfd, timeout_ms);
  if (result != node::NodeError::SUCCESS) {
    return result;
  }

  size_t enc_size = request.encode_size();
  std::vector<char> sendBuffer(enc_size);

  bool ret = request.encode(sendBuffer.data(), (u32)sendBuffer.size());
  if (!ret) {
    vlog_error(VCAT_NODE, "Error encoding");
    return node::NodeError::IDL_ENCODE_ERROR;
  }
  n = write(sockfd, sendBuffer.data(), enc_size);
  if (n != (ssize_t)enc_size) {
    vlog_error(VCAT_NODE, "Error sending request, wanted to send %ld bytes but only sent %zd bytes", enc_size,
               n);
    return node::NodeError::SOCKET_WRITE_ERROR;
  }

  struct timeval tv = SetTimeval(timeout_ms);
  int iret = setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
  if (iret < 0) {
    perror("Recv timeout");
  }

  if (!read_cbuf(sockfd, recvBuffer, n, timeout_ms)) {
    char thread_name[16];
    vlog_error(VCAT_NODE, "Error receiving a node reply from %s for %s by %s", server_ip.c_str(),
               RequestTypeToStr[int(request.action)], get_thread_name(thread_name));
    return node::NodeError::SOCKET_REPLY_ERROR;
  }
  ret = reply.decode(recvBuffer.data(), (u32)n);
  if (!ret) {
    vlog_error(VCAT_NODE, "Error decoding the received node reply from %s for %s", server_ip.c_str(),
               RequestTypeToStr[int(request.action)]);
    return node::NodeError::IDL_DECODE_ERROR;
  }

#if DEBUG
  printReply(reply);
#endif

  return node::NodeError::SUCCESS;
}

// expects expression 127.0.0.1:1337
// returns the number of components found, either 0 or 1 or 2
static int split_ipport(std::string& result_ip, uint16_t& result_port, const std::string& ip_colon_port) {
  // Alternative 1. https://stackoverflow.com/questions/2958747/what-is-the-nicest-way-to-parse-this-in-c
  // Alternative 2. https://rosettacode.org/wiki/Parse_an_IP_Address
  const char* colon = strchr(ip_colon_port.c_str(), ':');
  if (colon == nullptr) {
    result_ip = ip_colon_port;
    return 1;  // assume input is just the ip address, there is no port
  }
  char result_address[512];
  int result = sscanf(ip_colon_port.c_str(), "%s:%hu", result_address, &result_port);
  result_ip = result_address;
  return result;
}

node::NodeError send_request(const std::string& server_ip_port, node::registry_request& request,
                             node::registry_reply& reply, int timeout_ms) {
  std::string server_address;
  uint16_t server_port = DEFAULT_NODE_REGISTRY_PORT;
  int items = split_ipport(server_address, server_port, server_ip_port);
  if (items < 1) {
    char thread_name[16];
    vlog_error(VCAT_NODE,
               "expected expression 127.0.0.1:1337 for ipaddress and port.  %s could not be parsed by %s",
               server_ip_port.c_str(), get_thread_name(thread_name));
    return node::NodeError::GENERIC_ERROR;
  }
  return send_request_impl(server_address, server_port, request, reply, timeout_ms);
}

std::vector<u64> get_host_fingerprint() {
  std::vector<u64> vec;
  struct ifaddrs* ifaddr = NULL;
  struct ifaddrs* ifa = NULL;
  int i = 0;

  if (getifaddrs(&ifaddr) == -1) {
    vlog_error(VCAT_NODE, "Error on getifaddrs get_host_fingerprint: %d : %s", errno, strerror(errno));
  } else {
    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
      if ((ifa->ifa_addr) && (ifa->ifa_addr->sa_family == AF_PACKET) && !(ifa->ifa_flags & IFF_LOOPBACK)) {
        struct sockaddr_ll* s = (struct sockaddr_ll*)ifa->ifa_addr;
        // printf("%-8s ", ifa->ifa_name);
        u64 num = 0;
        for (i = 0; i < s->sll_halen; i++) {
          num += s->sll_addr[i] << i * 8;
          // printf("%02x%c", (s->sll_addr[i]), (i+1!=s->sll_halen)?':':'\n');
        }
        vec.push_back(num);
      }
    }
    freeifaddrs(ifaddr);
  }
  std::sort(vec.begin(), vec.end());
  return vec;
}

bool ip_in_list(const char* ip, const std::string& ip_list) {
  auto vec = node::split(ip_list, ';');
  for (auto& v : vec) {
    if (ip == v) return true;
  }
  return false;
}

namespace node {

std::vector<std::string> split(const std::string& s, char seperator) {
  std::vector<std::string> output;
  std::string::size_type prev_pos = 0, pos = 0;

  if (s.empty()) return output;

  while ((pos = s.find(seperator, pos)) != std::string::npos) {
    std::string substring(s.substr(prev_pos, pos - prev_pos));
    output.push_back(substring);
    prev_pos = ++pos;
  }

  output.push_back(s.substr(prev_pos, s.size() - prev_pos));  // Last word
  return output;
}

std::set<std::string> split_set(const std::string& s, char seperator) {
  std::set<std::string> output;
  std::string::size_type prev_pos = 0, pos = 0;

  if (s.empty()) return output;

  while ((pos = s.find(seperator, pos)) != std::string::npos) {
    std::string substring(s.substr(prev_pos, pos - prev_pos));
    output.insert(substring);
    prev_pos = ++pos;
  }

  output.insert(s.substr(prev_pos, s.size() - prev_pos));  // Last word
  return output;
}

void static_split(const std::string& s, char separator, char out[5][20]) {
  std::string::size_type prev_pos = 0, pos = 0;
  int idx = 0;

  while ((pos = s.find(separator, pos)) != std::string::npos) {
    strncpy(out[idx], &s.c_str()[prev_pos], (pos - prev_pos));
    idx++;
    prev_pos = ++pos;
  }

  VLOG_ASSERT(idx < 5);
  if (pos == std::string::npos) {
    pos = s.size();
  }

  strncpy(out[idx], &s.c_str()[prev_pos], (pos - prev_pos));  // Last word
  idx++;
}

bool find_ip_for_node(const std::string& server_ips, std::string& server_ip, uint16_t port) {
  char host_ips[20][20] = {};
  get_host_ips_vec(host_ips);
  char ipvec[5][20] = {};
  node::static_split(server_ips, ';', ipvec);
  bool connected = false;
  int sockfd = -1;

  // First, let's see if we have an IP on the same subnet
  std::string ip_same_subnet;
  for (auto& hostip : host_ips) {
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
    auto result = connect_with_timeout(ip_same_subnet, port, sockfd, 100, true);
    if (result == node::NodeError::SUCCESS) {
      connected = true;
      server_ip = ip_same_subnet;
      if (sockfd != -1) close(sockfd);
      return connected;
    } else {
      if (sockfd != -1) close(sockfd);
      sockfd = -1;
      vlog_fine(VCAT_NODE, "Tried connecting to server on ip %s:%u", ip_same_subnet.c_str(), port);
    }
  }

  for (auto& ip : ipvec) {
    if (ip[0] == 0) continue;
    auto result = connect_with_timeout(ip, port, sockfd, 100, true);
    if (result == node::NodeError::SUCCESS) {
      connected = true;
      server_ip = ip;
      if (sockfd != -1) close(sockfd);
      break;
    } else {
      if (sockfd != -1) close(sockfd);
      sockfd = -1;
      vlog_fine(VCAT_NODE, "Tried connecting to publisher on ip %s:%u, will try with next ip", ip, port);
    }
  }
  return connected;
}
}  // namespace node
