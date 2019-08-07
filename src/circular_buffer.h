#pragma once

#include <assert.h>
#include <semaphore.h>
#include <pthread.h>
#include <string.h>
#include <cmath>

#define VERBOSE_DEBUG 0

struct message_bookkeep
{
  // Refcount for the message
  unsigned int refcount = 0;
  // Is this message published or not?
  unsigned int published = 0;
};

// Constants for get_next_full()
constexpr float NODE_DEFAULT_MSG_WAIT_SEC = 3.0;  // Block for 3 sec
constexpr float NODE_NON_BLOCKING = 0.0;          // Don't block if empty

// This class expects to be allocated on shared memory 
class circular_buffer
{
    unsigned int  buf_size;
    unsigned int  head_;

    pthread_cond_t  buffer_cv;
    pthread_mutex_t buffer_lock;

public:
    void print_state(message_bookkeep *bk)
    {
#if VERBOSE_DEBUG
        int old_head = dec(head_);
        printf("\nhead: %d head_.refcount %d head_.published %d\n", head_, bk[head_].refcount, bk[head_].published);
        printf("oldhead: %d oldhead_.refcount %d oldhead_.published %d\n", old_head, bk[old_head].refcount, bk[old_head].published);
#endif
    }

    unsigned int inc(unsigned int v) const
    {
        return (v+1)%buf_size;
    }

    int dec(int v) const
    {
        return (v+buf_size-1)%buf_size;
    }

    void set_buf_size(unsigned int buf_size)
    {
        this->buf_size = buf_size;
    }

    unsigned int get_buf_size() const
    {
        return buf_size;
    }

    /// Get the index on the buffer of the next empty element, called by publisher
    /// This function will not block for now, ideally we never ran out of space
    unsigned int get_next_empty(message_bookkeep *bk)
    {
        pthread_mutex_lock(&buffer_lock);

        print_state(bk);

        // Alternatively we could block here
        if (bk[head_].refcount > 0) {
          assert(!"Not enough space, or someone did not release a buffer!!");
          printf("Node: buffer not release, overriding\n");
          bk[head_].refcount = 0;
        }
        bk[head_].published = 0;
        pthread_mutex_unlock(&buffer_lock);
        return head_;
    }

    /// This function will publish an item and advertise that
    void publish(message_bookkeep *bk)
    {
#if VERBOSE_DEBUG
        printf("Publish index: %d\n", head_);
#endif
        pthread_mutex_lock(&buffer_lock);
        bk[head_].published = 1;
        assert(bk[head_].refcount == 0);
        head_ = (head_ + 1) % buf_size;
        bk[head_].published = 0;

        print_state(bk);

        pthread_mutex_unlock(&buffer_lock);
        pthread_cond_broadcast(&buffer_cv);
    }

    /// This is a function called by subscribers, it finds out the next idx for the
    /// subscriber given which was the last index it checked (it could be -1 if none).
    ///
    unsigned int get_next_index(message_bookkeep *bk, int last_checked_idx)
    {
      int prev_head = dec(head_);
      if (last_checked_idx == -1) {
        // we try to return the latest, which would be head_ - 1.
        if (bk[prev_head].published == 1) {
          return prev_head;
        } else {
          // Handle the case where we do not have anything published yet
          return head_;
        }
      } else {
        int next_idx = inc(last_checked_idx);
        if (bk[next_idx].published == 1) {
          return next_idx;
        } else {
          // go and try with the -1 version
          return get_next_index(bk, -1);
        }
      }
    }

    // Function called by subscribers to see if there is new data
    bool is_index_available(message_bookkeep *bk, int idx)
    {
      return bk[idx].published == 1;
    }

    // last_checked_idx in this case is eitehr -1 or the index for the last elem checked
    // out by this subscriber. get_next_full will look for the very next if it exists
    // this is a BLOCKING call
    node::NodeError get_next_published(message_bookkeep *bk, int last_checked_idx, unsigned int &elem_index)
    {
        int next_idx;
        pthread_mutex_lock(&buffer_lock);

        print_state(bk);
        int prev_head = dec(head_);
        if (last_checked_idx == -1) {
          // we try to return the latest, which would be head_ - 1.
          if (bk[prev_head].published == 1) {
#if VERBOSE_DEBUG
            printf("Using prev_head: %d because it has been published\n", prev_head);
#endif
            bk[prev_head].refcount++;
            pthread_mutex_unlock(&buffer_lock);
            elem_index = prev_head;
            return node::SUCCESS;
          } else {
            // Handle the case where we do not have anything published yet
#if VERBOSE_DEBUG
            printf("Using the current head\n");
#endif
            next_idx = head_;
          }
        } else {
          next_idx = inc(last_checked_idx);
          if (bk[next_idx].published != 1) {
            if (bk[last_checked_idx].published != 1) {
              // Fallback here since the last checked index was not published, meaning we are lost and we reset
#if VERBOSE_DEBUG
              printf("Fallback to minus 1, the head. Last checked id was: %d, bk[last] = %d ; bk[next] = %d\n",
                     last_checked_idx, bk[last_checked_idx].published, bk[next_idx].published);
#endif
              // go and try with the -1 version
              pthread_mutex_unlock(&buffer_lock);
              return get_next_published(bk, -1, elem_index);
            }
            // Fall through to the case where we wait for next_idx
          }
        }

        if (bk[next_idx].published == 1) {
#if VERBOSE_DEBUG
            printf("Using next_id %d, last checked was %d\n", next_idx, last_checked_idx);
#endif
          bk[next_idx].refcount++;
          pthread_mutex_unlock(&buffer_lock);
          elem_index = next_idx;
          return node::SUCCESS;
        }
        pthread_cond_wait(&buffer_cv, &buffer_lock);
        assert(bk[next_idx].published == 1);
        bk[next_idx].refcount++;
        pthread_mutex_unlock(&buffer_lock);
        elem_index = next_idx;
        return node::SUCCESS;
    }

    // Subscribers call this, idx is the element index
    void release(message_bookkeep *bk, unsigned int idx)
    {
#if VERBOSE_DEBUG
        printf("Releasing index: %d\n", idx);
#endif                                        
        pthread_mutex_lock(&buffer_lock);

        print_state(bk);

        assert(bk[idx].refcount > 0);
        assert(bk[idx].published == 1);
        bk[idx].refcount--;
        pthread_mutex_unlock(&buffer_lock);
    }

    // This function is to be called by the producer only, to initialize the memory
    // and critical sections
    void initialize(unsigned int num_elems, message_bookkeep *bk)
    {
        buf_size = num_elems;
        head_ = 0;
        memset(bk, 0, sizeof(message_bookkeep)*num_elems);

        pthread_condattr_t cattr;
        pthread_condattr_init(&cattr);
        pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
        pthread_cond_init(&buffer_cv, &cattr);

        pthread_mutexattr_t mattr;
        pthread_mutexattr_init(&mattr);
        pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
        pthread_mutex_init(&buffer_lock, &mattr);

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

};
