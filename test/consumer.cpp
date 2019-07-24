#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include <assert.h>
#include "image.h"
#include "node/core.h"

int main(int argc, char **argv)
{
    node::core core;

    printf("Hello, I am a consumer of messages\n");

    auto image_channel = core.subscribe<node_msg::image>("topic");

    node::NodeError res = image_channel.open();
    if (res != node::SUCCESS) {
        u32 t = 0;
        while (res == node::PRODUCER_NOT_PRESENT) {
            // In this case, wait for some time until the producer starts
            // this could be a race condition in some cases
            usleep(2000000);
            t+=2;
            printf(".");
            if (t >= 20) {
                break;
            }
            res = image_channel.open();
        }

        if (res != node::SUCCESS) {
            fprintf(stderr, "Failed to open the topic: %d\n", res);
            return -1;

        }
    }

    unsigned int val = 0;
    bool first_val = false;

    printf("Now starting consumer \n"); fflush(stdout);
    for(int it=0; it<500; it++) {
        printf(" - Acquiring data (%d)... ", it);
        fflush(stdout);
        node_msg::image *img = image_channel.get_message(res);
        if (res != node::SUCCESS) {
            // Likely the producer is no longer around
            printf(" No data received in a while, terminating ...\n");
            break;
        }
        printf("Value of rows: %d (expected %d) \n", img->rows, val+1);
        fflush(stdout);
        if (!first_val) {
            val = img->rows;
            first_val = true;
        } else {
            
            // Very simple way to check that we are receiving in order and not missing
//             assert(img->rows == val +1);
            val = img->rows;
        }
        // maybe do something with img here

        printf(" releasing data ... ");
        fflush(stdout);
        image_channel.release_message( img );
        printf(" RELEASED!\n");
        usleep(10000);
    }
    return 0;
}
