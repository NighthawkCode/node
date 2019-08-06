#pragma once

namespace node {

enum NodeError
{
    SUCCESS = 0,
    GENERIC_ERROR                =  1,
    TOPIC_NOT_FOUND              =  2,
    OLD_PRODUCER_IS_STILL_ALIVE  =  3,
    SOCKET_COULD_NOT_BE_CREATED  =  4,
    SOCKET_SERVER_IP_INCORRECT   =  5,
    SOCKET_SERVER_NOT_FOUND      =  6,
    IDL_ENCODE_ERROR             =  7,
    SOCKET_WRITE_ERROR           =  8,
    SOCKET_REPLY_ERROR           =  9,
    IDL_DECODE_ERROR             = 10,
    INDEX_OUT_OF_BOUNDS          = 11,
    SERVER_INCOMPATIBLE          = 12,
    SHARED_MEMORY_OPEN_ERROR     = 13,
    CONSUMER_LIMIT_EXCEEDED      = 14,
    PRODUCER_NOT_PRESENT         = 15,
    CONSUMER_TIME_OUT            = 16,
    CBUF_MSG_NOT_SUPPORTED       = 17
};

}
