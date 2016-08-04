#ifndef MARKER_CACHE_H
#define MARKER_CACHE_H
#include "shmbloomfilter.h"

class marker_cache {
    // Suitable identification scheme for subtable division here
    // To replace the id scheme, simply change this typedef and provide a
    // comparator for the map, currently std::less<int>
    typedef int marker_cache_id;
    typedef std::less<int> id_comparator;

    typedef std::pair<const marker_cache_id, bf::shm_bloom_filter> bf_pair;
    typedef boost::interprocess::allocator<bf_pair, bf::segment_manager_t>
        bf_pair_allocator;
    typedef boost::interprocess::map<marker_cache_id, bf::shm_bloom_filter,
                                     id_comparator, bf_pair_allocator>
        id_bf_map;

    // API for managing shared memory and retrieving handles to data
    // Takes identifier of the bloom filter (per subtable, per marker type)
    // For our purposes, identifier is just the marker type (int32)
   public:
    // Create shared memory block allocating *bytes* bytes, writing process
    marker_cache(size_t bytes);

    // Throws an exception if the memory is not active, searching process
    marker_cache();

    ~marker_cache();
    // Forbid copy and move construction
    marker_cache(marker_cache const &) = delete;
    marker_cache &operator=(marker_cache const &) = delete;
    marker_cache(marker_cache &&) = delete;
    marker_cache &operator=(marker_cache &&) = delete;

    // Create new cache, size bits, fp false-positive rate
    // returns the number of bytes occupied by the new bloom filters
    size_t create(marker_cache_id id, double fp, size_t capacity,
                  size_t seed = 0);

    // Check if a bloom filter exists with this id
    bool exists(marker_cache_id id) const;

    // data_len is num of chars (bytes)
    // returns false if cache not found
    bool lookup_from(marker_cache_id id, char *data, int data_len) const;

    // returns true if success, false if cache not found
    bool insert_into(marker_cache_id id, char *data, int data_len);

    // Remove a bloom filter
    void remove(marker_cache_id id);

    // Erase all the bloom filters
    void erase();

   private:
    boost::interprocess::managed_shared_memory segment_;
    id_bf_map *data_;
    bf::void_allocator get_allocator();
    bool owner_;
};

#endif