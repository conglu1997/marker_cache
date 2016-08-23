#ifndef MARKER_CACHE_H
#define MARKER_CACHE_H
#include <shmbloomfilter.h>
#include <boost/interprocess/containers/deque.hpp>
#include <ctime>
#include <memory>

class marker_cache {
    typedef std::pair<time_t, time_t> timerange;

    typedef std::pair<timerange, bf::shm_bloom_filter> bf_pair;

    typedef bf::void_allocator::rebind<bf_pair>::other bf_pair_allocator;
    typedef boost::interprocess::deque<bf_pair, bf_pair_allocator> cache_buffer;

    // API for managing shared memory and retrieving handles to data
   public:
    // Bloom filter duration and lifespan are given in minutes
    marker_cache(size_t min_filterduration, size_t min_filterlifespan,
                 double fp, size_t total_capacity);

    // Throws an exception if the memory is not active, reading process
    marker_cache();

    // Clear shared memory on exit if the process owns the memory
    ~marker_cache();

    // Forbid copy construction
    marker_cache(marker_cache const &) = delete;
    marker_cache &operator=(marker_cache const &) = delete;

    // data_len is num of chars (bytes)
    bool lookup_from(time_t start, time_t end, char *data, int data_len) const;

    // Convenience function for testing
    bool lookup_from_current(char *data, int data_len) const;

    // Insert into the most recent Bloom filter
    void insert(char *data, int data_len);

    // DBAPP will call maybe_age() which can call age()
    void maybe_age();

    // Currently public for forcing an age
    void age();

   private:
    // The shared memory object
    boost::interprocess::managed_shared_memory *segment_;
    cache_buffer *data_;
    bf::void_allocator get_allocator();
    bool owner_;

    // Filter duration in seconds, specific to DBApp, won't be initialised on
    // the SD side
    time_t sec_filterduration;

    bool within_timerange(time_t time, timerange range) const;
};

#endif
