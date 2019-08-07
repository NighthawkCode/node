#include "node_registry.h"
#include "process.h"
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <linux/limits.h>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <streambuf>

static
bool file_exists( const char* filepath )
{
  struct stat status;
  if( stat( filepath, &status ) == 0 && S_ISREG( status.st_mode ) ) {
    return true;
  }
  return false;
}

namespace node {

u32 node_registry::num_topics()
{
    u32 number = 0;
    for(u32 i=0; i<topics.size(); i++) {
        if (topics[i].visible) number++;
    }
    return number;
}

u32 node_registry::num_topics_cli()
{
    return topics.size();
}

bool node_registry::get_topic_info(u32 topic_index, topic_info& info)
{
    if (topic_index >= num_topics()) return false;
    u32 number = 0;
    for(u32 i=0; i<topics.size(); i++) {
        if (topics[i].visible) {
            if (number == topic_index) {
                if (is_pid_alive(topics[i].publisher_pid)) {
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

bool node_registry::get_topic_info_cli(u32 topic_index, topic_info& info)
{
    if (topic_index >= num_topics_cli()) return false;
    info = topics[topic_index];
    return true;
}

bool node_registry::create_topic(const topic_info& info)
{
    for(u32 i=0; i<topics.size(); i++) {
        if (topics[i].name == info.name) {
            if (is_pid_alive(topics[i].publisher_pid)) {
                printf("PID of previous topic was alive, new one could not be created\n");
                return false;
            }

            topics[i] = info;
            topics[i].visible = false;
            return true;
        }
    }        
    topics.push_back(info);
    topics[topics.size()-1].visible = false;
    return true;
}

bool node_registry::make_topic_visible(const topic_info& info)
{
    for(auto& t: topics) {
        if (t.name == info.name) {
            t.visible = true;
            return true;
        }
    }
    return false;
}

bool node_registry::get_topic_info(const std::string& name, topic_info& info)
{
    for(auto& t: topics) {
        if (t.visible && t.name == name) {
            if (is_pid_alive(t.publisher_pid)) {
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

const std::string& node_registry::get_session_path()
{
    if (session_path.empty()) {
        time_t rawtime;
        struct tm *info;
        time( &rawtime );
        info = localtime( &rawtime );

        char robot_name[64] = {};

        strcpy(robot_name, "unknown");
        std::stringstream sbuffer;

        if (file_exists( "/etc/robot_name" )) {
          std::ifstream t("/etc/robot_name");
          sbuffer << t.rdbuf();
          std::string tstr = sbuffer.str();
          if (tstr[tstr.size()-1] == '\n') tstr[tstr.size()-1] = 0;
          strcpy(robot_name, tstr.c_str());
        }

        char buffer[PATH_MAX];
        memset(buffer, 0, sizeof(buffer));
        sprintf(buffer, "%s-%d.%02d.%02d.%02d.%02d.ulog", robot_name,
                info->tm_year + 1900, info->tm_mon + 1, info->tm_mday, info->tm_hour, info->tm_min );
        session_path = buffer;
    }
    return session_path;
}

}
