#pragma once
#include "mytypes.h"
#include <string>

class channel
{
    // Memory fields
    void* mem;
    u32   length;

    // Channel fields
    u64 hash;

public:
    // Open channel, returns the pointer if it worked
    void *open(const std::string& channel_path, u32 size);

    channel();
    ~channel();
};