
#include <stdio.h>  
#include <stdlib.h>     
#include <string.h>     
#include <thread>
#include <chrono>

//#include "posix.h"
//#include "sockets.h"
#include <unistd.h>			// standard symbolic constants and types
#include <sys/types.h>		// data types
#include <sys/socket.h>		// Core POSIX socket functions and data structures. socket() bind() listen() accept() connect()
#include <sys/poll.h>		// definitions for the poll() function
#include <sys/ioctl.h>		// I/O blocking or non-blocking control
#include <fcntl.h>			// posix file opening, locking and other control operations.
#include <net/if.h>			// sockets local interfaces
#include <netinet/in.h>		// Internet address family, Internet protocol family
#include <arpa/inet.h>		// Functions for manipulating numeric IP addresses. 
#include <resolv.h>			// res_query() Make queries to and interpret responses from Internet domain name servers.
#include <endian.h>			// POSIX htons()

#define HAIL_RESPOND_HOST   "localhost"
#define HAIL_RESPOND_PORT   3713

#define HAIL_MESSAGE  "Any Node?"
#define RESPOND_MESSAGE  "Node Ready"     
