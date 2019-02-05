#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include "image.h"
#include "channel.h"

int main(int argc, char **argv)
{
    topic_producer<node_msg::image> cn;

    printf("Hello, I am a producer of messages\n");

    // List existing topics

    NodeError res = cn.open_channel("topic");
    if (res != NE_SUCCESS) {
        // TODO: make it so the node server does not need restart
        fprintf(stderr, "Failure to create a topic (%d), maybe you need to restart the node server\n", res);
        return -1;
    }

    printf("Now starting publishing\n");
    for(int it = 0; it < 150; it++) {
        printf(" - Acquiring data (%d)... ", it);
        fflush(stdout);
        node_msg::image* img = cn.get_slot();
        printf("Previous valud of rows: %d ", img->rows);
        img->rows = 3+it;
        img->cols = 3+it;
        img->format = 12;
        img->timestamp = 0;
        for(int i=0; i<256; i++) img->pixels[i] = i*it;

        printf(" publishing data ... ");
        fflush(stdout);
        cn.publish();
        printf(" PUBLISHED!\n");
        usleep(500000);
    }

    return 0;
}