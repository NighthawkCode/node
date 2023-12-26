#include "node_registry.h"

#include <linux/limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <streambuf>
#include <string>

#include "node/node_utils.h"
#include "process.h"
#include "vlog.h"

static bool file_exists(const char* filepath) {
  struct stat status;
  if (stat(filepath, &status) == 0 && S_ISREG(status.st_mode)) {
    return true;
  }
  return false;
}

// Recursive function to perform wildcard matching between two null
// terminated strings. The only wildcard supported is '*'. This probably will
// not work properly if str has '*' present in it as this token is defined as
// wildcard
// Error Codes/Scenarios and edge cases:
//   This function does not handle escaping of wildcards. The behavior is undefined if
//   the str also has *
// Args:
//   wild: The pattern to match and contains wildcard
//   str: The string to check if it matches wild
static bool match(const char* wild, const char* str) {
  if (*wild == 0 && *str == 0) {
    return true;
  }
  if (*wild == '*') {
    if (match(wild + 1, str)) return true;
    if (*str == 0) return false;
    return match(wild, str + 1);
  }
  if (*wild == *str) {
    return match(wild + 1, str + 1);
  }
  return false;
}

namespace node {

u32 node_registry::num_topics() {
  std::lock_guard lk(mtx);
  u32 number = 0;
  for (u32 i = 0; i < topics.size(); i++) {
    if (topics[i].visible) number++;
  }
  return number;
}

u32 node_registry::num_topics_cli() { return (u32)topics.size(); }

bool node_registry::get_topic_info(u32 topic_index, topic_info& info) {
  std::lock_guard lk(mtx);
  if (topic_index >= num_topics()) return false;
  u32 number = 0;
  for (u32 i = 0; i < topics.size(); i++) {
    if (topics[i].visible) {
      if (number == topic_index) {
        if (topics[i].cn_info.is_network || is_pid_alive(topics[i].publisher_pid)) {
          info = topics[i];
          return true;
        } else {
          topics[i].visible = false;
          break;
        }
      }
      number++;
    }
  }
  return false;
}

bool node_registry::get_topic_info_cli(u32 topic_index, topic_info& info) {
  std::lock_guard lk(mtx);
  if (topic_index >= num_topics_cli()) return false;
  info = topics[topic_index];
  return true;
}

bool node_registry::create_topic(const topic_info& info) {
  std::lock_guard lk(mtx);
  for (u32 i = 0; i < topics.size(); i++) {
    if (topics[i].topic_name == info.topic_name) {
      if (info.cn_info.is_network) {
        auto hip = get_host_ips();
        if (info.cn_info.channel_ip != hip) {
          // Do not allow to recreate topics in network
          // One day we could check if the socket is still up
          vlog_error(VCAT_NODE,
                     "Registry: Could not create topic %s, already in the list and it is a network topic\n",
                     info.topic_name.c_str());
          return false;
        }
        // If the network publisher was on the same host, check pid, fall through
      }
      if (is_pid_alive(topics[i].publisher_pid)) {
        vlog_error(VCAT_NODE, "PID %d of previous topic was alive, new one could not be created\n",
                   topics[i].publisher_pid);
        return false;
      }

      topics[i] = info;
      topics[i].visible = false;
      return true;
    }
  }
  topics.push_back(info);
  if (!info.cn_info.is_network) {
    topics[topics.size() - 1].visible = false;
  }
  return true;
}

bool node_registry::make_topic_visible(const topic_info& info) {
  std::lock_guard lk(mtx);
  for (auto& t : topics) {
    if (t.topic_name == info.topic_name) {
      t.visible = true;
      return true;
    }
  }
  return false;
}

void node_registry::copy_all_topics_to(std::vector<topic_info>& dest) {
  std::lock_guard lk(mtx);
  dest = topics;
}

void node_registry::copy_network_topics_to(std::vector<topic_info>& dest,
                                           const std::unordered_set<std::string>& skip_list) {
  std::lock_guard lk(mtx);
  for (const auto& tp : topics) {
    if (tp.cn_info.is_network && skip_list.count(tp.topic_name) == 0) {
      dest.push_back(tp);
    }
  }
}

void node_registry::copy_network_topic_names_to_str(std::string& dest) {
  dest = "";
  std::lock_guard lk(mtx);
  for (const auto& tp : topics) {
    if (tp.cn_info.is_network) {
      if (!dest.empty()) dest += ";";
      dest += tp.topic_name;
    }
  }
}

bool node_registry::get_topic_info(const std::string& name, topic_info& info) {
  std::lock_guard lk(mtx);
  for (auto& t : topics) {
    if (t.visible && t.topic_name == name) {
      if (t.cn_info.is_network || is_pid_alive(t.publisher_pid)) {
        info = t;
        return true;
      } else {
        t.visible = false;
        break;
      }
    }
  }
  return false;
}

const std::string& node_registry::get_session_name() {
  if (session_name.empty()) {
    time_t rawtime;
    struct tm* info;
    time(&rawtime);
    info = localtime(&rawtime);

    char robot_name[64] = {};

    strcpy(robot_name, "unknown");

    if (file_exists("/etc/robot_name")) {
      std::ifstream t("/etc/robot_name");
      std::string first_word;
      // this will read the first word (defined as a set of contiguous non white-space characters)
      // if there is no word at all then it will evaluate to false
      if (t >> first_word) {
        strcpy(robot_name, first_word.c_str());
      } else {
        vlog_error(VCAT_NODE, "We could not read the /etc/robot_name file to get the robot name correctly");
      }
    }

    char buffer[PATH_MAX];
    memset(buffer, 0, sizeof(buffer));
    sprintf(buffer, "%s-%d.%02d.%02d.%02d.%02d.%02d.ulog", robot_name, info->tm_year + 1900, info->tm_mon + 1,
            info->tm_mday, info->tm_hour, info->tm_min, info->tm_sec);
    session_name = buffer;
  }
  return session_name;
}

bool node_registry::is_session_name_set() const {
  std::lock_guard lk(mtx);
  return !session_name.empty();
}

void node_registry::set_session_name(const std::string& sname) {
  std::lock_guard lk(mtx);
  session_name = sname;
}

bool node_registry::set_registry_ip(const std::string& ip) {
  if (!registry_ip.empty()) {
    vlog_error(VCAT_NODE, "The registry ip is already set to %s. Cannot reset it.", registry_ip.c_str());
    return false;
  }
  registry_ip = ip;
  return true;
}

std::string node_registry::get_log_path() {
  std::lock_guard lk(mtx);
  if (log_folder.empty()) {
    log_folder = ".";
  }
  return log_folder + "/" + get_session_name();
}

std::string node_registry::get_log_folder() {
  std::lock_guard lk(mtx);
  if (log_folder.empty()) {
    log_folder = ".";
  }

  char buf[4096];
  std::string real_path = realpath(log_folder.c_str(), buf);
  return real_path;
}

void node_registry::reset_log_path() {
  std::lock_guard lk(mtx);
  session_name.clear();
}

// We do not allow to change the folder once set
void node_registry::set_log_folder(const std::string& folder) {
  std::lock_guard lk(mtx);
  if (log_folder.empty()) {
    log_folder = folder;
  }
}

void node_registry::remove_topics_by_ip(const char* str) {
  std::lock_guard lk(mtx);
  for (auto it = topics.begin(); it != topics.end();) {
    auto& topic = *it;
    if (!topic.cn_info.is_network) {
      it++;
      continue;
    }
    if (ip_in_list(str, topic.cn_info.channel_ip)) {
      vlog_fine(VCAT_NODE, "[Nodemaster] Removing topic %s from peer %s (reconnect/disconnect)",
                topic.topic_name.c_str(), str);
      it = topics.erase(it);
    } else {
      it++;
    }
  }
}

std::string node_registry::get_value(const std::string& key) {
  std::lock_guard<std::mutex> lk(store_mtx);
  if (key_value_store.find(key) == key_value_store.end())
    return "";
  else
    return key_value_store[key].value;
}

std::string node_registry::get_owner(const std::string& key) {
  std::lock_guard<std::mutex> lk_s(store_mtx);
  if (key_value_store.find(key) == key_value_store.end()) return "";
  return key_value_store[key].owner;
}

bool node_registry::set_value(const std::string& key, const std::string& value, const std::string& owner) {
  std::lock_guard<std::mutex> lk(store_mtx);
  VLOG_ASSERT(!registry_ip.empty());
  if (key_value_store.find(key) == key_value_store.end()) {
    if (!owner.empty()) {
      key_value_store[key] = {.value = value, .owner = owner};
      return true;
    } else if (!registry_ip.empty()) {
      key_value_store[key] = {.value = value, .owner = registry_ip};
      return true;
    }
  } else if (key_value_store[key].owner == owner) {
    key_value_store[key].value = value;
    return true;
  }
  return false;
}

bool node_registry::update_store(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                                 const std::string& owner) {
  std::string owner_updated = owner;
  if (owner.empty()) {
    VLOG_ASSERT(!registry_ip.empty());
    owner_updated = registry_ip;
  }

  std::lock_guard<std::mutex> lk(store_mtx);
  // ensure that this can succeed
  VLOG_ASSERT(keys.size() == values.size());
  int num_events = (int)keys.size();
  bool can_update = true;
  for (int i = 0; i < num_events; i++) {
    if ((key_value_store.find(keys[i]) == key_value_store.end() ||
         key_value_store[keys[i]].owner == owner_updated)) {
      continue;
    }
    vlog_error(VCAT_NODE,
               "Ownership collison for (key, value, owner) (%s, %s, %s). Update requested with (key, value, "
               "owner) (%s, %s, %s)",
               keys[i].c_str(), key_value_store[keys[i]].value.c_str(),
               key_value_store[keys[i]].owner.c_str(), keys[i].c_str(), values[i].c_str(), owner.c_str());
    can_update = false;
  }

  if (can_update) {
    for (int i = 0; i < num_events; i++) {
      const auto& key = keys[i];
      const auto& value = values[i];
      key_value_store[key] = {value, owner_updated};
    }
    return true;
  } else {
    return false;
  }
}

// Make a copy of the current state and return it
void node_registry::get_current_store_snapshot(std::vector<std::string>& keys,
                                               std::vector<std::string>& values, bool all,
                                               const std::string& search_regex_term) {
  std::lock_guard<std::mutex> lk_s(store_mtx);
  VLOG_ASSERT(!registry_ip.empty());
  for (const auto& [key, value] : key_value_store) {
    if (all || value.owner == registry_ip) {
      if (search_regex_term.empty() || match(search_regex_term.c_str(), key.c_str())) {
        keys.push_back(key);
        values.push_back(value.value);
      }
    }
  }
}
}  // namespace node
