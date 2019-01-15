#pragma once
#include "mytypes.h"
#include <string>
#include "nodecore.h"
#include "circular_buffer.h"

void* helper_open_channel(const channel_info& info);

template< class T>
class topic
{
    circular_buffer indices; // Indices should be allocated inside of data... 
    u8* data = nullptr;

public:
        
        // this function will open the channel, allocate memory, set the indices, and
        // do any other needed initialization
        bool open_channel(const std::string &topic_name, bool producer)
        {
            bool bret = false;
            // Find the registry, inquire about this channel
            nodecore reg;
            topic_info info;

            bret = reg.open();
            if (!bret) {
                return false;
            }

            bret = reg.get_topic_info(topic_name, info);
            if (!bret) {
                if (producer) {
                    // New topic, let's create it
                    info.name = topic_name;
                    info.message_name = T::TYPE_STRING;
                    info.message_hash = T::TYPE_HASH;
                    info.cn_info.channel_path = "/node_";
                    info.cn_info.channel_path += info.message_hash;
                    info.cn_info.channel_size = 4096; // minimum size, 4 Kbyte
                    bret = reg.create_topic(info);
                    if (!bret) {
                        return false;
                    }
                } else {
                    // Consumer cannot create new topics
                    return false;
                }
            } 
            
            // Now we have the topic info on info
            data = (u8 *)helper_open_channel(info.cn_info);
            if (!data) {
                return false;
            }

            // do setup of stuff in data now!
            return true;
        }
        
        // This function will actually initialize the channel and put data on it
        bool prod_init(int num_elems = 10)
        {
            return false;
        }
        
        // Producer: get a pointer to a struct to fill
        T* prod_get_slot()
        {
            // This call might block
            unsigned int elem_index = indices.get_next_empty();
            
            T* inner_data = (T *)data;
            return &inner_data[elem_index];
        }
        
        // Producer: This function assumes that the image* previously returned will no longer be used
        void prod_publish()
        {
            indices.publish();
        }
        
        // Consumer: get a pointer to the next struct from the publisher
        T* cons_get_slot()
        {
            // This call might block
            // TODO: how to handle multiple consumers?
            unsigned int elem_index = indices.get_next_full();
            
            T* inner_data = (T *)data;
            return &inner_data[elem_index];
        }
        
        // Consumer: This function assumes that the image* previously returned will no longer be used
        void cons_release()
        {
            indices.release();
        }
        
        // This function will do a resize .. TO BE DONE
        bool resize()
        {
            return true;
        }

};

