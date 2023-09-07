#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "node/nodeerr.h"
#include "node/types.h"
#include "registry.h"

namespace node {

///
///  These set of funtions provide a way to query a node master
///
///

// This function will access the current node system.
NodeError check_master(const std::string& host = "");

// Get the number of open channels on the system
NodeError num_channels(u32& outval, const std::string& host = "");

// This function retrieves the channel info based on the index, the
// info parameter is output. The function returns false if there is no
// channel on that index
NodeError get_topic_info(u32 channel_index, topic_info& info, const std::string& host = "");

// Get all the topics
NodeError get_all_topic_info(std::vector<topic_info>& vec, const std::string& host = "");

// Create a new channel on the system, with the information on info
NodeError create_topic(const topic_info& info, const std::string& host = "");

// Get information on a topic on the system, based on the name of the topic.
// returns false if there is no topic with that name
NodeError get_topic_info(const std::string& name, topic_info& info, const std::string& host = "");

NodeError make_topic_visible(const std::string& name, const std::string& host = "");

// This function has no error return, it will just return the empty string on error
std::string get_session_name(const std::string& host = "");

// This function has no error return, it will just return the empty string on error
std::string get_log_path(const std::string& host = "");

// This function returns the parent folder where ulogs are stored, without creating a new ulog
std::string get_log_folder(const std::string& host = "");

NodeError reset_log_path(const std::string& host = "");

// This function will get the log path, but from a peer (any peer)
std::string get_peer_log_path(const std::string& host = "");

NodeError get_nodemaster_info(double& uptime, int& nm_pid, std::string& process_name,
                              std::vector<std::string>& active_peers);

// This function will request nodemaster to terminate, it is important to pass the who string for traceability
NodeError request_nodemaster_termination(const std::string& who, const std::string& host = "");
NodeError request_nodemaster_shutdown(const std::string& who, const std::string& host = "");

const char* NodeErrorToStr(node::NodeError err);

NodeError set_value(const std::string& key, const std::string& value, const std::string& host = "");

NodeError get_value(const std::string& key, std::string& value, std::string& owner,
                    const std::string& host = "");

NodeError get_store(std::vector<std::string>& keys, std::vector<std::string>& values,
                    const std::string& host = "", bool all = false,
                    const std::string& search_regex_term = "");

NodeError send_update_store_to_peers(const std::string& host = "");
}  // namespace node
