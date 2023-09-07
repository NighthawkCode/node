
#include <set>

#include "node/nodedefault.h"
#include "node/nodeerr.h"
#include "node/types.h"
#include "registry.h"

#define BUFFER_SIZE 8196

#define DEBUG 0

void printRequest(const node::registry_request& request, int vlog_level);
void printReply(const node::registry_reply& reply, int vlog_level);
const char* NodeRequestToStr(node::RequestType rt);
std::string get_host_ips();
bool get_host_ips_vec(char ips[20][20]);
bool same_subnet(const char* ip1, const char* ip2);

bool read_cbuf(int sockfd, std::vector<char>& buffer, ssize_t& bytes, int timeout_ms);
node::NodeError connect_with_timeout(const std::string& server_ip, uint16_t port, int& sockfd,
                                     int timeout_ms = 3000, bool blocking_socket = true);
node::NodeError send_request(const std::string& server_ip_port, node::registry_request& request,
                             node::registry_reply& reply, int timeout_ms = 3000);
bool ip_in_list(const char* ip, const std::string& ip_list);
std::vector<u64> get_host_fingerprint();

namespace node {
std::vector<std::string> split(const std::string& s, char seperator);
std::set<std::string> split_set(const std::string& s, char seperator);
void static_split(const std::string& s, char separator, char out[5][20]);

const char* NodeErrorToStr(node::NodeError err);

bool find_ip_for_node(const std::string& server_ips, std::string& server_ip,
                      int port = DEFAULT_NODE_REGISTRY_PORT);
}  // namespace node
