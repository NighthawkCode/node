#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include "image.h"
#include "channel.h"

int main(int argc, char **argv)
{
    topic_consumer<node_msg::image> cn;

    printf("Hello, I am a consumer of messages\n");

    if (!cn.open_channel("topic")) {
        fprintf(stderr, "Failed to open the topic\n");
        return -1;
    }

    printf("Now starting consumer\n");
    for(int it=0; it<150; it++) {
        printf(" - Acquiring data (%d)... ", it);
        fflush(stdout);
        node_msg::image *img = cn.get_slot();

        // maybe do something with img here
//        usleep(500000);

        printf(" releasing data ... ");
        fflush(stdout);
        cn.release();
        printf(" RELEASED!\n");
    }
    return 0;
}