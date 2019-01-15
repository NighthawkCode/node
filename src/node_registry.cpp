#include "node_registry.h"

u32 node_registry::num_topics()
{
    return topics.size();
}

bool node_registry::get_topic_info(u32 topic_index, topic_info& info)
{
    if (topic_index >= num_topics()) return false;
    info = topics[topic_index];
    return true;
}

bool node_registry::create_topic(const topic_info& info)
{
    topic_info tmp;
    if (get_topic_info(info.name, tmp)) {
        // duplicate!
        return false;
    }
    topics.push_back(info);
    return true;
}

bool node_registry::get_topic_info(const std::string& name, topic_info& info)
{
    for(auto t: topics) {
        if (t.name == name) {
            info = t;
            return true;
        }
    }
    return false;
}
