#pragma once

#include <assert.h>
#include <semaphore.h>

#define MAX_CONSUMERS 10

// This class expects to be allocated on shared memory 
class circular_buffer
{
    unsigned int buf_size;
    unsigned int head_;
    unsigned int num_cons;

    unsigned int tail_[MAX_CONSUMERS];
    bool         valid[MAX_CONSUMERS];
    bool         full_[MAX_CONSUMERS];
//    bool         full_ = false;

    // these two variables are used for producer/consumer
    // If we support multiple consumer, we would have more pairs    
    sem_t        prod_sem[MAX_CONSUMERS]; 
    sem_t        cons_sem[MAX_CONSUMERS]; 

public:

    unsigned int inc(unsigned int v) const
    {
        return (v+1)%buf_size;
    }

    bool match(unsigned int head, unsigned int tail) const
    {
        return inc(head) == tail;
    }

    void set_buf_size(unsigned int buf_size)
    {
        this->buf_size = buf_size;
    }

    /// This function will uniquely identify different consumers
    unsigned int get_cons_number() {
        return __atomic_fetch_add(&num_cons, 1, __ATOMIC_SEQ_CST);
    }

    /// Returns wether the buffer is empty or not.
    bool empty() const
    {
        bool is_empty = true;
        if (full()) return false;
        // The buffer is only empty when all valid consumers have head == tail
        for(int i=0; i<MAX_CONSUMERS; i++) {
            if (valid[i] && match(head_, tail_[i])) {
                is_empty = false;
            }
        }
        return is_empty;
    }

    /// Returns wether the buffer is empty or not for this consumer
    bool empty_for_this_consumer(unsigned int idx) const
    {
        assert(valid[idx]);
        if (full_[idx]) return false;
        return head_ == tail_[idx];
    }

    /// Returns wether the buffer is full or not.
    bool full() const
    {
        bool is_full = false;       

        for(int i=0; i<MAX_CONSUMERS; i++) {
            if (valid[i] && full_[i]) {
                is_full = true;                
            }
        }
        return is_full;
//        return full_;
    }

/*
    // commenting this function because it is not clear it is needed
    // and the size of the buffer in a multi consumer scenario is not 
    // clearly defined

    /// Returns how many valid items are there in the buffer.
    unsigned int size() const
    {
        unsigned int sz = buf_size;

        if (!full_) {
            if (head_ >= tail_) {            for(int i=0; i<MAX_CONSUMERS; i++) {
                if (valid[i] && full_[i]) {
                    sem_wait(&cons_sem[i]);
                }
            }

                sz = head_ - tail_;
            } else {
                sz = buf_size + head_ - tail_;
            }
        }            sem_wait(&cons_sem);


        return sz;
    }
*/

    /// Get the index on the buffer of the next empty element.
    /// This function blocks if the buffer is full
    unsigned int get_next_empty()
    {
//        if (full()) {

        for(int i=0; i<MAX_CONSUMERS; i++) {
            if (valid[i] && full_[i]) {
                sem_wait(&cons_sem[i]);
            }
        }
        return head_;
    }

    /// This function will add an item at the top
    void publish()
    {
//        if (full()) {
            // Not Block
            // Do we still want to block here and not on next?
            // sem_wait(&cons_sem);
//        }
        // TODO: mutex for this?
        head_ = (head_ + 1) % buf_size;
        // Update full, see if tail and head meet
  //      full_ = head_ == tail_;
        for(int i=0; i<MAX_CONSUMERS; i++) {
            if (valid[i]) {
                full_[i] = (head_ == tail_[i]);
                sem_post(&prod_sem[i]);
            }
        }
    }

    unsigned int get_next_full(unsigned int idx)
    {
        if (empty_for_this_consumer(idx)) {
            // TODO: Block, add timeout and retry
            sem_wait(&prod_sem[idx]);
        }
        return tail_[idx];
    }

    void release(unsigned int idx)
    {
//        if (empty()) {
            // TODO: Block
            // Do we need to do this?

//        }
        full_[idx] = false;
        tail_[idx] = inc(tail_[idx]);
        sem_post(&cons_sem[idx]);
    }

    // This function is to be called by the producer only, to initialize the memory
    // and critical sections
    void initialize(unsigned int num_elems)
    {
        buf_size = num_elems;
        num_cons = 0;
        head_ = 0;
        for(unsigned int i=0; i< MAX_CONSUMERS; i++) {
            tail_[i] = 0;
            valid[i] = false;
            full_[i] = false;
        }
//        full_ = false;

        int err;

        for(unsigned int i=0; i< MAX_CONSUMERS; i++) {
            err = sem_init(&cons_sem[i], 1, 0);
            if (err != 0) {
                fprintf(stderr, "Error on initializing the cons semaphore\n");
            }
            err = sem_init(&prod_sem[i], 1, 0);
            if (err != 0) {
                fprintf(stderr, "Error on initializing the cons semaphore\n");
            }
        }
    }

    void initialize_consumer(unsigned int idx)
    {
        tail_[idx] = head_;
        valid[idx] = true;
    }
};