#pragma once

#include "nodelib.h"
#include <vector>
#include <string>

namespace node {

class node_registry
{
    std::vector<topic_info> topics;
    std::string session_path;

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

    const std::string& get_session_path();
};

}
