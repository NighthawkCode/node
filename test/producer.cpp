#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include "image.h"
#include "node/core.h"

int main(int argc, char **argv)
{
    node::core core;

    const char* topic = "topic";
    if( argc == 2 ) {
        topic = argv[1];
    }
    printf("Hello, I am a producer of messages. I will publish topic: %s\n", topic );

    auto image_channel = core.provides<node_msg::image>(topic);

    // List existing topics

    node::NodeError res = image_channel.open(100);
    if (res != node::SUCCESS) {
        // TODO: make it so the node server does not need restart
        fprintf(stderr, "Failure to create a topic (%d), maybe you need to restart the node server\n", res);
        return -1;
    }

    // Wait a bit so consumers can attach if needed
    usleep(2000000);
    printf("Now starting publishing\n");
    for(int it = 0; it < 50000; it++) {
        printf(" - Acquiring data (%d)... ", it);
        fflush(stdout);
        node_msg::image* img = image_channel.prepare_message();
        printf("Previous value of rows: %d ", img->rows);
        img->rows = it;
        img->cols = 3+it;
        img->format = 12;
        img->timestamp = 0;
        for(int i=0; i<256; i++) img->pixels[i] = i*it;

        printf(" publishing data (%d) ... ", it);
        fflush(stdout);
        image_channel.transmit_message( img );
        printf(" PUBLISHED!\n");
        usleep(330000);
    }

    return 0;
}

