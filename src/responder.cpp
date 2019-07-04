#include "responder.h"
#include "hail_respond.h"

namespace node {

static bool responder_quit = false;
static std::thread responder_thread;

// Like responding "Yep, I'm here."
static 
void responder_thread_proc()
{
    unsigned short broadcastPort = HAIL_RESPOND_PORT;

    // Create UDP socket
    int sock;
    if ((sock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
        printf("socket() failed");
    }

    // Bind to the broadcast port 
    struct sockaddr_in broadcastAddr; 
    memset(&broadcastAddr, 0, sizeof(broadcastAddr));   
    broadcastAddr.sin_family = AF_INET;                 
    broadcastAddr.sin_addr.s_addr = htonl(INADDR_ANY);  // Any incoming interface 
    broadcastAddr.sin_port = htons(broadcastPort);      // Broadcast port 
    if (bind(sock, (struct sockaddr *) &broadcastAddr, sizeof(broadcastAddr)) < 0) {
        printf("bind() failed");
    }

    // Set NON-blocking socket
    if( 0 > fcntl( sock, F_SETFL, O_NONBLOCK ) ) {
        printf( "error: could not set NON-blocking \n" );
    }

    while( !responder_quit ) {
        int milliseconds = 200;

        pollfd pfd;
        pfd.fd = sock;
        pfd.events = POLLIN; 
        pfd.revents = 0;
        int pollresult = poll( &pfd, 1, milliseconds);
        if( pollresult == -1 ) {
            // error
        }
        else if( pollresult == 0 ) {
            // timeout
        }
        else
        {   
            if( pfd.revents & POLLIN  )  {             
                struct sockaddr_in Sender_addr;  
                int len = sizeof(struct sockaddr_in);

                const int MAXRECVSTRING = 2048;
                char recvString[MAXRECVSTRING+1]; 
                int recvStringLen = 0;            

                // Receive a broadcast.  And get the sender's address.
                if ((recvStringLen = recvfrom(sock, recvString, MAXRECVSTRING, 0, (sockaddr*)&Sender_addr, (socklen_t*) &len)) < 0) {
                    printf("responder error: recvfrom() failed");
                }
                else {
                    recvString[recvStringLen] = '\0';
                    //char str[INET_ADDRSTRLEN];
                    //inet_ntop(AF_INET, &(Sender_addr.sin_addr), str, INET_ADDRSTRLEN);
                    //printf("responder received from %s: %s\n", str, recvString); 

                    // Send an acknowledgement to the broadcaster.  
                    char respondmessage[]= RESPOND_MESSAGE;
                    if(sendto(sock, respondmessage, strlen(respondmessage)+1, 0, (sockaddr*)&Sender_addr, sizeof(Sender_addr) ) < 0 ) {
                        printf( "responder error: failed to send acknowledegement \n" );
                    }
                }
            }
        }
    }

    close(sock);
    return;
}


void responder::start_node_responder()
{
    responder_quit = false;
    responder_thread = std::thread( responder_thread_proc );
}

void responder::stop_node_responder()
{
    responder_quit = true;
    if( responder_thread.joinable() ) {
        responder_thread.join();
    }
}


}  // namespace node
