#pragma once
#include "mytypes.h"
#include <string>

// A channel is identified by a string (like a topic), and the type of messages on it (string, hash)
// channel messages are decoded
struct channel_info
{
    std::string name;
    std::string message_name;
    std::string channel_path;
    u64         message_hash;
};

/// This class handles the registration of channels in a system
class nodecore
{
public:
    // This function will access the current node system.
    bool open();

    // Get the number of open channels on the system
    u32 num_channels();

    // This function retrieves the channel info based on the index, the 
    // info parameter is output. The function returns false if there is no 
    // channel on that index
    bool get_channel_info(u32 channel_index, channel_info& info);

    // Create a new channel on the system, with the information on info
    bool create_channel(const channel_info& info);

    // Get information on a channel on the system, based on the name of the channel.
    // returns false if there is no channel with that name
    bool get_channel_info(const std::string& name, channel_info& info);
};