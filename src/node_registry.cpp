#include "node_registry.h"
#include "process.h"

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
    for(auto t: topics) {
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

}
