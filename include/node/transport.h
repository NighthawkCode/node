#pragma once
#include <string>
#include "node/nodeerr.h"
#include "mytypes.h"
#include "nodelib.h"
#include "circular_buffer.h"

namespace node {

void* helper_open_channel(const channel_info& info, int& mem_fd);
void  helper_clean(void *addr, int mem_fd, u32 mem_length);

template< class T>
class publisher
{
    circular_buffer *indices = nullptr; // Indices should be allocated inside of data... 
    u8* data = nullptr;
    int mem_fd = 0;
    u32 mem_length = 0;
    T*  elems = nullptr;
    std::string topic_name;

public:

    publisher() = default;
        
    publisher(publisher<T>&& rhs) : topic_name(std::move(rhs.topic_name))
    {
        indices = rhs.indices;
        data = rhs.data;
        mem_fd = rhs.mem_fd;
        mem_length = rhs.mem_length;
        elems = rhs.elems;

        rhs.indices = nullptr;
        rhs.data = nullptr;
        rhs.elems = nullptr;
        rhs.mem_fd = 0;
    }

    void set_topic_name(const std::string& name)
    {
        topic_name = name;
    }
    
    // this function will open the channel, allocate memory, set the indices, and
    // do any other needed initialization
    NodeError open(int num_elems = 4, unsigned int max_consumers = 5)
    {
        NodeError res;
        // Find the registry, inquire about this channel
        nodelib node_lib;
        topic_info info;

        res = node_lib.open();
        if (res != SUCCESS) {
            return res;
        }

        u32 sz = (sizeof(circular_buffer) + num_elems*sizeof(T));
        int reminder = sz / 1024;
        if (reminder != 0) {
            sz = sz + 1024 - reminder;
        }

        if (max_consumers >= MAX_CONSUMERS) {
            return CONSUMER_LIMIT_EXCEEDED;
        }

        info.name = topic_name;
        info.message_name = T::TYPE_STRING;
        info.message_hash = T::TYPE_HASH;
        info.cn_info.channel_path = "/node_";
        info.cn_info.channel_path += std::to_string(info.message_hash);
        info.cn_info.channel_size = sz;
        info.cn_info.max_consumers = max_consumers;
        info.visible = false;
        res = node_lib.create_topic(info);
        if (res != SUCCESS) {
            return res;
        }
        printf("New topic created successfully\n");
        
        // Now we have the topic info on info
        data = (u8 *)helper_open_channel(info.cn_info, mem_fd);
        if (!data) {
            printf("Could not open the shared memory\n");
            return SHARED_MEMORY_OPEN_ERROR;
        }

        mem_length = info.cn_info.channel_size;

        // do setup of stuff in data now!
        indices = (circular_buffer *)data;
        elems = (T *)( (u8 *)data + sizeof(circular_buffer));
        indices->initialize(num_elems);

        res = node_lib.make_topic_visible(topic_name);

        return res;
    }
        
    // Producer: get a pointer to a struct to fill
    T* get_slot()
    {
        // This call might block
        unsigned int elem_index = indices->get_next_empty();
        return &elems[elem_index];
    }
    
    // Producer: This function assumes that the image* previously returned will no longer be used
    void publish( T* elem )
    {
        indices->publish();
    }
            
    // This function will do a resize .. TO BE DONE
    bool resize()
    {
        return true;
    }

    ~publisher()
    {
        helper_clean(data, mem_fd, mem_length);
        data = nullptr;
        mem_length = 0;
        mem_fd = 0;
        elems = nullptr;
        indices = nullptr;
    }
};

template< class T>
class subscriber
{
    circular_buffer *indices = nullptr; // Indices are allocated inside of data
    u32 cons_index = 0; // Indicate which consumer this is, out of multiple in a topic
    u8* data = nullptr;
    int mem_fd = 0;
    u32 mem_length = 0;
    T*  elems = nullptr;  

    std::string topic_name;
public:
    subscriber() = default;

    subscriber(subscriber<T>&& rhs) : topic_name(std::move(rhs.topic_name))
    {
        indices = rhs.indices;
        cons_index = rhs.cons_index;
        data = rhs.data;
        mem_fd = rhs.mem_fd;
        mem_length = rhs.mem_length;
        elems = rhs.elems;

        rhs.indices = nullptr;
        rhs.data = nullptr;
        rhs.elems = nullptr;
        rhs.mem_fd = 0;
    }

    void set_topic_name(const std::string& name)
    {
        topic_name = name;
    }

    // this function will open the channel, allocate memory, set the indices, and
    // do any other needed initialization
    NodeError open()
    {
        NodeError res;
        // Find the registry, inquire about this channel
        nodelib node_lib;
        topic_info info;

        res = node_lib.open();
        if (res != SUCCESS) {
            fprintf(stderr, "Failure to open the node registry\n");
            return res;
        }

        res = node_lib.get_topic_info(topic_name, info);
        if (res != SUCCESS) {
            // Consumer cannot create new topics
            fprintf(stderr, "Failure to find the topic in the node registry\n");
            if (res == TOPIC_NOT_FOUND) {
                return PRODUCER_NOT_PRESENT;
            }
            return res;
        } 
        
        // Now we have the topic info on info
        data = (u8 *)helper_open_channel(info.cn_info, mem_fd);
        if (!data) {
            fprintf(stderr, "Failure to open the shared memory\n");
            return SHARED_MEMORY_OPEN_ERROR;
        }

        mem_length = info.cn_info.channel_size;

        printf("Opened channel with %d length, %s path", mem_length, 

            info.cn_info.channel_path.c_str());

        // do setup of stuff in data now!
        indices = (circular_buffer *)data;
        elems = (T *)( (u8 *)data + sizeof(circular_buffer));        

        cons_index = indices->get_cons_number();

        if (cons_index >= info.cn_info.max_consumers) {
            return CONSUMER_LIMIT_EXCEEDED;
        }

        indices->initialize_consumer(cons_index);
        this->topic_name = topic_name;

        return SUCCESS;
    }
            
    // Consumer: get a pointer to the next struct from the publisher
    T* get_slot(NodeError &result)
    {
        // This call might block
        unsigned int elem_index;
        result = indices->get_next_full(cons_index, elem_index);
        
        if (result == SUCCESS) {
            return &elems[elem_index];
        }
        // The most likely problem here is that the producer died, maybe check one day
        return nullptr;
    }
    
    // Consumer: This function assumes that the image* previously returned will no longer be used
    void release( T* elem )
    {
        indices->release(cons_index);
    }
    
    // This function will do a resize .. TO BE DONE
    bool resize()
    {
        return true;
    }

    u32 get_index() const 
    {
        return cons_index;
    }

    ~subscriber()
    {
        helper_clean(data, mem_fd, mem_length);
        data = nullptr;
        mem_length = 0;
        mem_fd = 0;
        elems = nullptr;
        indices = nullptr;
    }

};

}
