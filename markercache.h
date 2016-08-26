#ifndef MARKER_CACHE_H
#define MARKER_CACHE_H
#include <shmbloomfilter.h>
#include <boost/interprocess/containers/deque.hpp>
#include <boost/interprocess/sync/interprocess_sharable_mutex.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <ctime>
#include <memory>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/filesystem.hpp>
#include <boost/serialization/utility.hpp>
#include <fstream>
#include <sstream>

#include <boost/log/core.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>

class marker_cache {
    // Internal-only representation of timeranges
    typedef std::pair<time_t, time_t> timerange;
    struct bf_pair {
        bf_pair(const bf::void_allocator &void_alloc) : second(void_alloc) {}
        bf_pair(timerange f, bf::shm_bloom_filter s) : first(f), second(s) {}
        timerange first;
        bf::shm_bloom_filter second;

        friend class boost::serialization::access;
        template <class Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar &first;
            ar &second;
        }
    };

    typedef bf::void_allocator::rebind<bf_pair>::other bf_pair_allocator;
    typedef boost::interprocess::deque<bf_pair, bf_pair_allocator> cache_buffer;

    // API for managing shared memory and retrieving handles to data
   public:
    // Bloom filter duration and lifespan are given in minutes then converted to
    // seconds for internal use
    // The duration is long the Bloom filter is active for
    // The lifespan is how long the Bloom filter is held in memory for before
    // being deleted
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

    // Insert into the most recent Bloom filter
    void insert(char *data, int data_len);

    // DBAPP will call maybe_age() which can call age()
    // Takes a boolean parameter to force an ageing cycle, this should only be
    // used for testing purposes
    // Take a parameter to disable automatic saving
    void maybe_age(bool force = false);

    // Do a disk write of Bloom filters which have not been saved already
    void save();

   private:
    // The shared memory object
    boost::interprocess::managed_shared_memory *segment_;
    cache_buffer *buf_;
    bf::void_allocator get_allocator();
    bool owner_;

    // Filter duration in seconds, specific to DBApp, won't be initialised on
    // the SD side
    time_t sec_filterduration;

    bool overlapping_timerange(timerange fst, timerange snd) const;

    boost::filesystem::path timestamp_to_filepath(time_t t);

    boost::interprocess::interprocess_sharable_mutex *mutex;

    boost::filesystem::path archive_dir;

    boost::log::sources::severity_logger<boost::log::trivial::severity_level>
        lg;
};

#endif