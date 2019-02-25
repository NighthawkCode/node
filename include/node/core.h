#pragma once

/*
 * Core header of Node
 * 
 */

#include <string>
#include "node/transport.h"

namespace node {

class core {
    public:
        void configure( const std::string& cfg );
        
        template<typename T> 
        publisher<T> provides ( const std::string& topic_name )
        {
            publisher<T> pub;
            pub.set_topic_name(topic_name);
            return pub;
        }
        
        template<typename T> 
        subscriber<T> subscribe( const std::string& topic_name )
        {
            subscriber<T> sub;
            sub.set_topic_name(topic_name);
            return sub;
        }

        void activate(); 
};

}