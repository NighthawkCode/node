#pragma once

#include <semaphore.h>

// This class expects to be allocated on shared memory 
class circular_buffer
{
    unsigned int buf_size;
    unsigned int head_ = 0;
    unsigned int tail_ = 0;
    bool         full_ = false;

    // these two variables are used for producer/consumer
    // If we support multiple consumer, we would have more pairs    
    sem_t        prod_sem; 
    sem_t        cons_sem; 

public:

    void set_buf_size(unsigned int buf_size)
    {
        this->buf_size = buf_size;
    }

    /// Reset the buffer, mark everything empty.
    void reset()
    {
      head_ = tail_;
      full_ = false;
    }

    /// Returns wether the buffer is empty or not.
    bool empty() const
    {
        return (!full_ && (head_ == tail_));
    }

    /// Returns wether the buffer is full or not.
    bool full() const
    {
        return full_;
    }

    /// Returns how many valid items are there in the buffer.
    unsigned int size() const
    {
        unsigned int sz = buf_size;

        if (!full_) {
            if (head_ >= tail_) {
                sz = head_ - tail_;
            } else {
                sz = buf_size + head_ - tail_;
            }
        }

        return sz;
    }

    /// Get the index on the buffer of the next empty element.
    /// This function blocks if the buffer is full
    unsigned int get_next_empty()
    {
        if (full()) {
            sem_wait(&cons_sem);
        }
        return head_;
    }

    /// This function will add an item at the top
    void publish()
    {
        if (full()) {
            // Not Block
            // Do we still want to block here and not on next?
            // sem_wait(&cons_sem);
        }
        // TODO: mutex for this?
        head_ = (head_ + 1) % buf_size;
        // Update full, see if tail and head meet
        full_ = head_ == tail_;
        sem_post(&prod_sem);
    }

    unsigned int get_next_full()
    {
        if (empty()) {
            // TODO: Block
            sem_wait(&prod_sem);
        }
        return tail_;
    }

    void release()
    {
        if (empty()) {
            // TODO: Block
            // Do we need to do this?

        }
        full_ = false;
        tail_ = (tail_ + 1) % buf_size;
        sem_post(&cons_sem);
    }

    void initialize(unsigned int num_elems)
    {
        buf_size = num_elems;
        head_ = 0;
        tail_ = 0;
        full_ = false;

        int err = sem_init(&prod_sem, 1, 0);
        if (err != 0) {
            fprintf(stderr, "Error on initializing the prod semaphore\n");
        }

        err = sem_init(&cons_sem, 1, 0);
        if (err != 0) {
            fprintf(stderr, "Error on initializing the cons semaphore\n");
        }
    }
};