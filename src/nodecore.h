#pragma once
#include "mytypes.h"
#include <string>

// A channel is identified by a string (like a topic), and the type of messages on it (string, hash)
// channel messages are decoded
struct channel_info 
{
    std::string channel_path;
    u32         channel_size;
    // Add information on TCP / UDP / SHMEM
};

struct topic_info 
{
    std::string  name;
    std::string  message_name;
    u64          message_hash;
    channel_info cn_info;
};

/// This class handles the registration of topics in a system
class nodecore
{
    std::string hostname;
public:
    // This function will access the current node system.
    bool open(const std::string& hostname = "");

    // Get the number of open channels on the system
    u32 num_channels();

    // This function retrieves the channel info based on the index, the 
    // info parameter is output. The function returns false if there is no 
    // channel on that index
    bool get_topic_info(u32 channel_index, topic_info& info);

    // Create a new channel on the system, with the information on info
    bool create_topic(const topic_info& info);

    // Get information on a topic on the system, based on the name of the topic.
    // returns false if there is no topic with that name
    bool get_topic_info(const std::string& name, topic_info& info);

    ~nodecore();
};