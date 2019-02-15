#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include <assert.h>
#include "image.h"
#include "channel.h"

int main(int argc, char **argv)
{
    topic_consumer<node_msg::image> cn;

    printf("Hello, I am a consumer of messages\n");

    NodeError res = cn.open_channel("topic");
    if (res != NE_SUCCESS) {
        u32 t = 0;
        while (res == NE_PRODUCER_NOT_PRESENT) {
            // In this case, wait for some time until the producer starts
            // this could be a race condition in some cases
            usleep(2000000);
            t+=2;
            printf(".");
            if (t >= 20) {
                break;
            }
            res = cn.open_channel("topic");
        }

        if (res != NE_SUCCESS) {
            fprintf(stderr, "Failed to open the topic: %d\n", res);
            return -1;

        }
    }

    unsigned int val = 0;
    bool first_val = false;

    printf("Now starting consumer [%d] \n", cn.get_index());
    for(int it=0; it<50; it++) {
        printf(" - Acquiring data (%d)... ", it);
        fflush(stdout);
        node_msg::image *img = cn.get_slot(res);
        if (res != NE_SUCCESS) {
            // Likely the producer is no longer around
            printf(" No data received in a while, terminating ...\n");
            break;
        }
        printf("Value of rows: %d ", img->rows);
        if (!first_val) {
            val = img->rows;
            first_val = true;
        } else {
            // Very simple way to check that we are receiving in order and not missing
            assert(img->rows == val +1);
            val = img->rows;
        }
        // maybe do something with img here
        usleep(5000000);

        printf(" releasing data ... ");
        fflush(stdout);
        cn.release();
        printf(" RELEASED!\n");
    }
    return 0;
}