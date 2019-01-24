#include "node_registry.h"
#include "process.h"

u32 node_registry::num_topics()
{
    u32 number = 0;
    for(u32 i=0; i<topics.size(); i++) {
        if (topics[i].visible) number++;
    }
    return number;
}

bool node_registry::get_topic_info(u32 topic_index, topic_info& info)
{
    if (topic_index >= num_topics()) return false;
    u32 number = 0;
    for(u32 i=0; i<topics.size(); i++) {
        if (topics[i].visible && (number == topic_index)) {
            info = topics[i];
            return true;
        }
    }
    return false;
}

bool node_registry::create_topic(const topic_info& info)
{
    topic_info tmp;
    if (get_topic_info(info.name, tmp)) {
        // duplicate!
        // Let's see if the publisher process is still alive or not
        if (is_pid_alive(tmp.publisher_pid)) {
            printf("PID of previous topic was alive, new one could not be created\n");
            return false;
        }
        // If it is not alive, we have to overwire it
        for(u32 i=0; i<topics.size(); i++) {
            if (topics[i].name == info.name) {
                topics[i] = info;
                return true;
            }
        }        
    }
    topics.push_back(info);
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
    for(auto t: topics) {
        if (t.visible && t.name == name) {
            info = t;
            return true;
        }
    }
    return false;
}
