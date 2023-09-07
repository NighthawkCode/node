#pragma once

#include <assert.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>

#include <cmath>

#include "node/nodeerr.h"
#include "vlog.h"

#define VERBOSE_DEBUG 0

enum class MessageState { EMPTY = 0, PUBLISHED = 1, STALE = 2 };

struct message_bookkeep {
  // Refcount for the message
  unsigned int refcount = 0;
  // This is an enum containing
  // EMPTY     : default, means the message has no contents
  // PUBLISHED : when the publisher has written the message and can be consumed
  // STALE     : when a subscriber holds the message for long and should be skipped
  MessageState state = MessageState::EMPTY;

  bool is_published() const { return state == MessageState::PUBLISHED; }
  bool is_stale() const { return state == MessageState::STALE; }
  inline void mark_stale() { state = MessageState::STALE; }
  inline void mark_published() { state = MessageState::PUBLISHED; }
  inline void mark_empty() { state = MessageState::EMPTY; }
};

// This class expects to be allocated on shared memory
class circular_buffer {
  unsigned int buf_size = 0;
  unsigned int head_ = 0;
  uint64_t num_packets = 0;
  unsigned int aligned_elem_size_ = 0;

  pthread_cond_t buffer_cv;
  pthread_mutex_t buffer_lock;

  static inline timespec TimeNow(double offset = 0.0) {
    constexpr long BILLION_NSECS = 1000000000L;

    struct timespec ts;
    auto r = clock_gettime(CLOCK_REALTIME, &ts);
    VLOG_ASSERT(r != -1);
    (void)r;

    double whole;
    const double frac = modf(offset, &whole);
    ts.tv_sec += long(offset);
    ts.tv_nsec += long(frac * 1e9);
    if (ts.tv_nsec >= BILLION_NSECS) {
      ts.tv_sec += ts.tv_nsec / BILLION_NSECS;
      ts.tv_nsec %= BILLION_NSECS;
    }

    return ts;
  }

public:
  void print_state(message_bookkeep* bk) {
#if VERBOSE_DEBUG
    int old_head = dec(head_);
    printf("\nhead: %d head_.refcount %d head_.published %d\n", head_, bk[head_].refcount,
           bk[head_].published);
    printf("oldhead: %d oldhead_.refcount %d oldhead_.published %d\n", old_head, bk[old_head].refcount,
           bk[old_head].published);
#else
    (void)bk;
#endif
  }

  int sprint_state(message_bookkeep* bk, char* str, size_t sz) {
    int old_head = dec(head_);
    return snprintf(str, sz,
                    "head: %d head_.refcount %d head_.published %d oldhead: %d oldhead_.refcount %d "
                    "oldhead_.published %d",
                    head_, bk[head_].refcount, bk[head_].state, old_head, bk[old_head].refcount,
                    bk[old_head].state);
  }

  unsigned int get_aligned_elem_size() const { return aligned_elem_size_; }

  unsigned int inc(unsigned int v) const { return (v + 1) % buf_size; }

  int dec(int v) const { return (v + buf_size - 1) % buf_size; }

  void set_buf_size(unsigned int buf_size) { this->buf_size = buf_size; }

  unsigned int get_buf_size() const { return buf_size; }

  uint64_t get_num_packets() const { return num_packets; }

  /// Get the index on the buffer of the next empty element, called by publisher
  /// This function will not block for now, ideally we never ran out of space
  unsigned int get_next_empty(message_bookkeep* bk) {
    pthread_mutex_lock(&buffer_lock);

    print_state(bk);

    auto old_head = head_;

    // Check if a subscriber is still holding memory
    while (bk[head_].refcount > 0) {
      // Mark it as stale
      bk[head_].mark_stale();
      // try to go to the next one
      head_ = inc(head_);
      // This asserts protects against fully running out of buffer memory
      VLOG_ASSERT(old_head != head_, "We ran out of message memory in shm");
    }
    // This will clear the published and stale state
    bk[head_].mark_empty();
    pthread_mutex_unlock(&buffer_lock);
    return head_;
  }

  /// This function will publish an item and advertise that
  void publish(message_bookkeep* bk) {
#if VERBOSE_DEBUG
    printf("Publish index: %d\n", head_);
#endif
    {
      // Wait at most X seconds. Maybe tweak this?
      const timespec ts = TimeNow(1.0);
      if (0 != pthread_mutex_timedlock(&buffer_lock, &ts)) {
        fprintf(stderr, "Error, published timed out trying to get the buffer lock\n");
        return;
      }
    }
    bk[head_].mark_published();
    VLOG_ASSERT(bk[head_].refcount == 0);
    head_ = inc(head_);

    print_state(bk);

    __atomic_fetch_add(&num_packets, 1, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock(&buffer_lock);
    pthread_cond_broadcast(&buffer_cv);
  }

  /// This is a function called by subscribers, it finds out the next idx for the
  /// subscriber given which was the last index it checked (it could be -1 if none).
  ///
  unsigned int get_next_index(message_bookkeep* bk, int last_checked_idx) {
    int prev_head = dec(head_);
    if (last_checked_idx == -1) {
      // we try to return the latest, which would be head_ - 1.
      while (prev_head != head_) {
        if (bk[prev_head].is_published()) {
          return prev_head;
        } else if (bk[prev_head].is_stale()) {
          prev_head = dec(prev_head);
          // continue
        } else {
          break;
        }
      }
      // Handle the case where we do not have anything published yet
      return head_;
    } else {
      int next_idx = inc(last_checked_idx);
      while (next_idx != head_) {
        if (bk[next_idx].is_stale()) {
          next_idx = inc(next_idx);
          // continue
        } else {
          break;
        }
      }
      // we return a valid index, but might not be published
      return next_idx;
    }
  }

  // Function called by subscribers where they try to guess how many indices are
  // there between last_checked_idx and the head
  unsigned int get_num_indices(message_bookkeep* bk, int last_checked_idx) {
    if (last_checked_idx == -1) {
      unsigned int idx = get_next_index(bk, last_checked_idx);
      return is_index_available(bk, idx) ? 1 : 0;
    }
    unsigned int num = 0;
    int idx = inc(last_checked_idx);
    while (idx != head_) {
      if (bk[idx].is_published()) {
        num++;
      }
      idx = inc(idx);
    }
    return num;
  }

  // Function called by subscribers to see if there is new data
  // published means available unless the head points to it
  bool is_index_available(message_bookkeep* bk, int idx) { return idx != head_ && bk[idx].is_published(); }

  // Function called by subscribers at the start to set a reference point for last index
  // This might be a non-published index
  int get_starting_last_index(message_bookkeep* bk) {
    int last_idx = dec(head_);
    while (bk[last_idx].is_stale() && last_idx != head_) {
      last_idx = dec(last_idx);
    }
    return last_idx;
  }

  // last_checked_idx in this case is eitehr -1 or the index for the last elem checked
  // out by this subscriber. get_next_full will look for the very next if it exists
  // this is a BLOCKING call
  node::NodeError get_next_published(message_bookkeep* bk, int last_checked_idx, unsigned int& elem_index,
                                     float timeout_sec = NODE_DEFAULT_MSG_WAIT_SEC) {
    int next_idx;

    {
      // Wait at most X seconds. Maybe tweak this?
      const timespec ts = TimeNow(1.0);
      if (0 != pthread_mutex_timedlock(&buffer_lock, &ts)) {
        return node::CONSUMER_TIME_OUT;
      }
    }

    print_state(bk);
    int prev_head = get_starting_last_index(bk);
    if (last_checked_idx == -1) {
      // we try to return the latest, which would be head_ - 1.
      if (bk[prev_head].is_published()) {
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
      if (bk[next_idx].is_stale()) {
        // the publisher wrapped around, reset
        // Fallback here since the last checked index was not published, meaning we are lost and we reset
#if VERBOSE_DEBUG
        printf("Fallback to minus 1, the head. Last checked id was: %d, bk[last] = %d ; bk[next] = %d\n",
               last_checked_idx, bk[last_checked_idx].published, bk[next_idx].published);
#endif
        // go and try with the -1 version
        pthread_mutex_unlock(&buffer_lock);
        return get_next_published(bk, -1, elem_index, timeout_sec);
      }
    }

    if (bk[next_idx].is_published()) {
#if VERBOSE_DEBUG
      printf("Using next_id %d, last checked was %d\n", next_idx, last_checked_idx);
#endif
      bk[next_idx].refcount++;
      pthread_mutex_unlock(&buffer_lock);
      elem_index = next_idx;
      return node::SUCCESS;
    }

#if VERBOSE_DEBUG
    printf("Going to wait for next_idx %d to become available\n", next_idx);
#endif

    {
      const timespec ts = TimeNow(timeout_sec);
      if (0 != pthread_cond_timedwait(&buffer_cv, &buffer_lock, &ts)) {
        pthread_mutex_unlock(&buffer_lock);
        return node::CONSUMER_TIME_OUT;
      }
    }
    VLOG_ASSERT(bk[next_idx].is_published());
    bk[next_idx].refcount++;
    pthread_mutex_unlock(&buffer_lock);
    elem_index = next_idx;
    return node::SUCCESS;
  }

  // Subscribers call this, idx is the element index
  // return values:
  // true: the release worked
  // false: the release worked but we had the lock too long, this is a major issue to report
  bool release(message_bookkeep* bk, unsigned int idx) {
#if VERBOSE_DEBUG
    printf("Releasing index: %d\n", idx);
#endif
    {
      // Wait at most X seconds. Maybe tweak this?
      const timespec ts = TimeNow(1.0);
      if (0 != pthread_mutex_timedlock(&buffer_lock, &ts)) {
        fprintf(stderr, "Warning, we timed out on release and should not have\n");
        return false;
      }
    }

    print_state(bk);

    bk[idx].refcount--;
    // We want to report stale cases
    bool ret = !bk[idx].is_stale();
    pthread_mutex_unlock(&buffer_lock);
    return ret;
  }

  // This function is to be called by the producer only, to initialize the memory
  // and critical sections
  void initialize(unsigned int num_elems, unsigned int aligned_elem_size, message_bookkeep* bk) {
    buf_size = num_elems;
    aligned_elem_size_ = aligned_elem_size;
    head_ = 0;
    num_packets = 0;
#if defined(__GNUC__) && !defined(__llvm__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
    memset(bk, 0, sizeof(message_bookkeep) * num_elems);
#if defined(__GNUC__) && !defined(__llvm__)
#pragma GCC diagnostic pop
#endif

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
