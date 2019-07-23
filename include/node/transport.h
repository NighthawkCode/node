#pragma once
#include <string>
#include <unistd.h>
#include "node/nodeerr.h"
#include "mytypes.h"
#include "nodelib.h"
#include "hailer.h"
#include "circular_buffer.h"

namespace node {

void* helper_open_channel(const channel_info& info, int& mem_fd);
void  helper_clean(void *addr, int mem_fd, u32 mem_length);

class publisher_base {
public:
  virtual ~publisher_base() {}
};

template< class T>
class publisher : public publisher_base
{
    circular_buffer *indices = nullptr; // Indices should be allocated inside of data... 
    u8* data = nullptr;
    int mem_fd = 0;
    u32 mem_length = 0;
    T*  elems = nullptr;
    std::string topic_name;
    const hailer& hail = get_hailer();

public:
    publisher() = default; 
    publisher(const std::string& topic_name) : topic_name(topic_name) {}
        
    publisher(publisher<T>&& rhs) :topic_name(std::move(rhs.topic_name))
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

    publisher &operator=(publisher<T>&& rhs) 
    {
        topic_name = std::move(rhs.topic_name);
        indices = rhs.indices;
        data = rhs.data;
        mem_fd = rhs.mem_fd;
        mem_length = rhs.mem_length;
        elems = rhs.elems;

        rhs.indices = nullptr;
        rhs.data = nullptr;
        rhs.elems = nullptr;
        rhs.mem_fd = 0;

        return *this;
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
    T* prepare_message()
    {
        // This call might block
        unsigned int elem_index = indices->get_next_empty();
        return &elems[elem_index];
    }
    
    // Producer: This function assumes that the image* previously returned will no longer be used
    void transmit_message( T* elem )
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

// Constants for subscriber::open()
constexpr float NODE_WAIT_FOREVER = -1.0;        // Retry indefinitely
constexpr float NODE_NO_RETRY = 0.0;             // Fail if producer not ready
constexpr float NODE_DEFAULT_RETRY_SECS = 2.0;   // Default retry duration

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

    subscriber &operator=(subscriber<T>&& rhs) 
    {
        topic_name = std::move(rhs.topic_name);
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

        return *this;
    }

    void set_topic_name(const std::string& name)
    {
        topic_name = name;
    }

    // this function will open the channel, allocate memory, set the indices, and
    // do any other needed initialization
    NodeError open(float timeout_sec = NODE_WAIT_FOREVER,
                   float retry_delay_sec = NODE_DEFAULT_RETRY_SECS)
    {
        NodeError res;
        // Find the registry, inquire about this channel
        nodelib node_lib;
        topic_info info;

        assert(retry_delay_sec > 0.0 && retry_delay_sec <= 60.0);

        res = node_lib.open();
        if (res != SUCCESS) {
            fprintf(stderr, "Failure to open the node registry for topic %s\n",
                    topic_name.c_str());
            return res;
        }

        res = node_lib.get_topic_info(topic_name, info);
        if (res == TOPIC_NOT_FOUND && timeout_sec != NODE_NO_RETRY) {
          float remaining_sec = timeout_sec;
          printf("Topic %s not found.  Retrying.", topic_name.c_str());
          while (res == TOPIC_NOT_FOUND &&
                 (timeout_sec == NODE_WAIT_FOREVER || remaining_sec > 0)) {
            printf(".");
            fflush(stdout);
            usleep(static_cast<useconds_t>(retry_delay_sec*1e6));
            remaining_sec -= retry_delay_sec;
            res = node_lib.get_topic_info(topic_name, info);
          }
          printf("\n");
        }
        if (res != SUCCESS) {
          // Consumer cannot create new topics
          fprintf(stderr, "Failure to find the topic %s in the node registry\n",
                  topic_name.c_str());
          if (res == TOPIC_NOT_FOUND) {
            return PRODUCER_NOT_PRESENT;
          }
          return res;
        } 
        
        // Now we have the topic info on info
        data = (u8 *)helper_open_channel(info.cn_info, mem_fd);
        if (!data) {
            fprintf(stderr, "Failure to open the shared memory for topic %s\n",
                    topic_name.c_str());
            return SHARED_MEMORY_OPEN_ERROR;
        }

        mem_length = info.cn_info.channel_size;

        printf("Opened channel %s with %d length, %s path\n", topic_name.c_str(),
               mem_length, info.cn_info.channel_path.c_str());

        // do setup of stuff in data now!
        indices = (circular_buffer *)data;
        elems = (T *)( (u8 *)data + sizeof(circular_buffer));        

        cons_index = indices->get_cons_number();

        printf("Got topic %s consumer index: %d, limit: %d\n", topic_name.c_str(),
               cons_index, info.cn_info.max_consumers);
        fflush(stdout);

        if (cons_index >= info.cn_info.max_consumers) {
            return CONSUMER_LIMIT_EXCEEDED;
        }

        indices->initialize_consumer(cons_index);

        return SUCCESS;
    }
            
    // Consumer: get a pointer to the next struct from the publisher
    T* get_message(NodeError &result,
                   float timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC)
    {
        // This call might block
        unsigned int elem_index;
        result = indices->get_next_full(cons_index, elem_index, timeout_sec);
        
        if (result == SUCCESS) {
            return &elems[elem_index];
        }
        // The most likely problem here is that the producer died, maybe check one day
        return nullptr;
    }
    
    // Consumer: This function assumes that the image* previously returned will no longer be used
    void release_message( T* elem )
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
