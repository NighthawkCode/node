#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "node/nodelib.h"

namespace node {

struct store_value {
  std::string value;
  std::string owner;  // ip of the owner
};

struct store_record {
  std::string key;
  store_value value_record;
};

class node_registry {
  std::vector<topic_info> topics;
  std::string log_folder;
  std::string session_name;
  std::string registry_ip;
  std::unordered_map<std::string, store_value> key_value_store;
  // use the same order of declaration if you want to acquire both the mutexes and avoid deadlock
  std::mutex store_mtx;
  mutable std::recursive_mutex mtx;

public:
  // Get the number of open topics on the system
  u32 num_topics();

  // Get the number of open topics on the system
  u32 num_topics_cli();

  // This function retrieves the topic info based on the index, the
  // info parameter is output. The function returns false if there is no
  // topic on that index
  bool get_topic_info(u32 topic_index, topic_info& info);

  bool get_topic_info_cli(u32 topic_index, topic_info& info);

  // Create a new topic on the system, with the information on info
  bool create_topic(const topic_info& info);

  // Get information on a topic on the system, based on the name of the topic.
  // returns false if there is no topic with that name
  bool get_topic_info(const std::string& name, topic_info& info);

  // Find the topic and make it visible
  bool make_topic_visible(const topic_info& info);

  void copy_all_topics_to(std::vector<topic_info>& dest);
  void copy_network_topics_to(std::vector<topic_info>& dest,
                              const std::unordered_set<std::string>& skip_list);
  void copy_network_topic_names_to_str(std::string& dest);

  bool is_session_name_set() const;
  void set_session_name(const std::string& sname);
  bool set_registry_ip(const std::string& ip);
  const std::string& get_session_name();
  std::string get_log_path();
  std::string get_log_folder();
  void set_log_folder(const std::string& folder);
  void reset_log_path();
  void remove_topics_by_ip(const char* str);
  // Functions to get and set key value store
  std::string get_value(const std::string& key);
  bool set_value(const std::string& key, const std::string& value, const std::string& owner = "");
  bool update_store(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                    const std::string& owner = "");
  void get_current_store_snapshot(std::vector<std::string>& keys, std::vector<std::string>& values,
                                  bool all = false, const std::string& search_term = "");
  std::string get_owner(const std::string& key);
};
}  // namespace node
