#include <stdio.h>
#include <stdlib.h>
#include "image.h"
#include "channel.h"

int main(int argc, char **argv)
{
    topic<node_msg::image> cn;

    printf("Hello, I am a producer of messages\n");

    cn.open_channel("topic", true);
    cn.prod_init(); 

    for(int it = 0; it < 150; it++) {
        node_msg::image* img = cn.prod_get_image();
        img->rows = 3+it;
        img->cols = 3+it;
        img->format = 12;
        img->timestamp = 0;
        for(int i=0; i<256; i++) img->pixels[i] = i*it;

        cn.prod_publish();
    }

    return 0;
}