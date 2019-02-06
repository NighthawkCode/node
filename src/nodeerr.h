#pragma once

enum NodeError
{
    NE_SUCCESS = 0,
    NE_GENERIC_ERROR                =  1,
    NE_TOPIC_NOT_FOUND              =  2,
    NE_OLD_PRODUCER_IS_STILL_ALIVE  =  3,
    NE_SOCKET_COULD_NOT_BE_CREATED  =  4,
    NE_SOCKET_SERVER_IP_INCORRECT   =  5,
    NE_SOCKET_SERVER_NOT_FOUND      =  6,
    NE_IDL_ENCODE_ERROR             =  7,
    NE_SOCKET_WRITE_ERROR           =  8,
    NE_SOCKET_REPLY_ERROR           =  9,
    NE_IDL_DECODE_ERROR             = 10,
    NE_INDEX_OUT_OF_BOUNDS          = 11,
    NE_SERVER_INCOMPATIBLE          = 12,
    NE_SHARED_MEMORY_OPEN_ERROR     = 13,
    NE_CONSUMER_LIMIT_EXCEEDED      = 14,
    NE_PRODUCER_NOT_PRESENT         = 15,
    NE_CONSUMER_TIME_OUT            = 16
};