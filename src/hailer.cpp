#include "hailer.h"
#include "hail_respond.h"
#include "nodelib.h"
#include "registry.h"
#include <unistd.h>
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "vlog.h"

namespace node {

const hailer& get_hailer()
{ 
    static hailer hail;
    return hail;
}

// Like saying "Hello?  Anyone there?"
bool hailer::is_node_running() const
{
    const char* hailhost = HAIL_RESPOND_HOST;            
    unsigned short hailport = HAIL_RESPOND_PORT; 
    const char* hailmessage = HAIL_MESSAGE;             

    // Create UDP socket
    int sock;
    if ((sock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
        vlog_error(VCAT_GENERAL, "hailer error: socket() failed");
        return false;
    }

    // Set socket to allow broadcast
    int permission = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_BROADCAST, (void*)&permission, sizeof(permission)) < 0) {
        vlog_error(VCAT_GENERAL, "hailer error: setsockopt() failed");
        close(sock);
        return false;
    }

    // Set NON-blocking socket
    if( 0 > fcntl( sock, F_SETFL, O_NONBLOCK ) ) {
        vlog_error(VCAT_GENERAL, "hailer error: could not set NON-blocking \n" );
        close(sock);
        return false;
    }

    struct sockaddr_in broadcastAddr; 
    memset(&broadcastAddr, 0, sizeof(broadcastAddr));   
    broadcastAddr.sin_family = AF_INET;                 
    broadcastAddr.sin_addr.s_addr = inet_addr(hailhost);// Broadcast IP address
    broadcastAddr.sin_port = htons(hailport);         // Broadcast port


    int maxloop = 5;
    for(int count=0; count < maxloop; count++)  {
        // Broadcast to anyone listening.
        unsigned int hailmessageLen = strlen(hailmessage);  
        if( sendto(sock, hailmessage, hailmessageLen, 0, (struct sockaddr*) &broadcastAddr, sizeof(broadcastAddr)) != hailmessageLen) {
            //printf("hailer sendto() sent a different number of bytes than expected");
        }

        std::this_thread::sleep_for( std::chrono::milliseconds(100) ); 

        int milliseconds = 100;
        pollfd pfd;
        pfd.fd = sock;
        pfd.events = POLLIN; 
        pfd.revents = 0;
        int pollresult = poll( &pfd, 1, milliseconds );
        if( pollresult > 0 )
        {
            if( pfd.revents & POLLIN  )  { 
                struct sockaddr_in Sender_addr;  
                int len = sizeof(struct sockaddr);
                const int MAXRECVSTRING = 2048;
                char recvString[MAXRECVSTRING+1]; 
                int recvStringLen = 0;        

                // Was there an acknowledgement?  
                if ((recvStringLen = recvfrom(sock, recvString, MAXRECVSTRING, 0, (struct sockaddr*) &Sender_addr, (socklen_t*) &len)) < 0) {
                    vlog_error(VCAT_GENERAL, "hailer error: recvfrom() failed");                    
                }
                else {
                    recvString[recvStringLen] = '\0';

                    // Just for debugging
                    //char str[INET_ADDRSTRLEN];
                    //inet_ntop(AF_INET, &(Sender_addr.sin_addr), str, INET_ADDRSTRLEN);
                    //vlog_info(VCAT_GENERAL, "hailer: received from %s: %s\n", str, recvString); 

                    if( 0 == strcmp(recvString, RESPOND_MESSAGE) ) {
                        
                        // Success.  Node is running.  And break out of this loop.
                        close(sock);
                        return true;  
                    }
                }
            }
        }
    }
    
    close(sock);
    return false;
}

extern
int nodecore_main();

extern
void nodecore_main_quit();

extern
NodeError send_request(const std::string &server_ip, 
                         node_msg::registry_request &request, 
                         node_msg::registry_reply &reply);


static std::thread node_thread;


std::string read_file_into_string( const std::string& filename )
{
    FILE* f = fopen(filename.c_str(), "r");
    if (f == nullptr) {
        vlog_error(VCAT_GENERAL, "read_file_into_string() -- could not open file %s for reading\n", filename.c_str());
        return "";
    }
    fseek(f, 0, SEEK_END);
    auto size = ftello64(f);
    char* buf = (char*)malloc(size+1);
    if (!buf) {
        vlog_error(VCAT_GENERAL, "read_file_into_string() -- failed to allocate %lu bytes\n", size);
        fclose(f);
        return "";
    }
    buf[size] = 0;
    fseek(f, 0, SEEK_SET);
    auto bytes_read = fread(buf, 1, size, f);
    if ((long int)bytes_read != size) {
        vlog_error(VCAT_GENERAL, "read_file_into_string() -- failed to read required bytes from file %s\n", filename.c_str());
        fclose(f);
        free(buf);
        return "";
    }
    std::string res(buf);
    free(buf);
    fclose(f);
    return res;
}

void hailer::run_node() const
{
    bool run_in_separate_process = false;

    // read node config file
    std::string jsontext = read_file_into_string("nodeconfig.json");
    if( !jsontext.empty() ) {
        rapidjson::Document json;
        rapidjson::ParseResult ok = json.Parse(jsontext.c_str());
        if (ok){
            const char* prop = "separate_process";
            if( json.HasMember(prop) && json[prop].IsBool() ){
                run_in_separate_process = json[prop].GetBool();
            }
        }
    }

    if( !run_in_separate_process ) { 
        // start node in this process
        if( !node_thread.joinable() ) {
            node_thread = std::thread( [](){
                nodecore_main();
            } );
        }
    }
    else 
    {
        // launch node in its own process
        pid_t childpid = fork();
        if( childpid == 0 )
        {
            // The executables are expected to be installed, and location is in the system PATH.
            const char* command = "nodesvr"; 
            const char* const argv[] = { command, nullptr };
            char** envp = environ;

            // On success, execve() does not return, on error -1 is returned.
			int ret = execve( command, (char* const*) argv, (char* const*) envp );
			if( ret == -1 ) {
				int errsv = errno;
				vlog_error(VCAT_GENERAL, "execve failed %d\n", errsv );
			}
            _exit(127);  // Exit child, command not found error
        }
        else if( childpid < 0 )
        {
            vlog_error(VCAT_GENERAL, "Error: %s(%d): could not fork. %s\n", __FUNCTION__, __LINE__, strerror(errno));
            return;
        }
        else if( childpid != 0 )
        {
            // This is the parent
            std::this_thread::sleep_for( std::chrono::milliseconds(500) ); 
        }
    }
}

void hailer::exit_node() const
{
    if( node_thread.joinable() ) {
        nodecore_main_quit();
        node_msg::registry_request req = {};
        node_msg::registry_reply reply = {};
        req.action = node_msg::NUM_TOPICS;
        req.cli = 123;
        std::string host = "localhost";
        send_request(host, req, reply);
        node_thread.join();
        vlog_info(VCAT_GENERAL, "node_thread joined success\n");
    }
}

}  // namespace node


