#ifndef MARKER_CACHE_H
#define MARKER_CACHE_H
#include "shm_bloom_filter.h"

typedef int bloom_filter_id;
// Suitable identification scheme for subtable division here

class marker_cache {
    // API for managing shared memory and retrieving handles to data
    // Takes identifier of the bloom filter (per subtable, per marker type)
    // For our purposes, identifier is just the marker type (int32)
   public:
    // max 4.2 billion bytes (unsigned integer)
    marker_cache(size_t bytes);
    ~marker_cache();
    // Create new cache, size bits, fp false-positive rate
    // max 4.2 billion items
    void create(bloom_filter_id id, double fp, size_t capacity, size_t seed = 0,
                bool double_hashing = true, bool partition = true);

    // data_len is num of chars (bytes)
    bool lookup_from(bloom_filter_id id, char *data, int data_len) const;

    void insert_into(bloom_filter_id id, char *data, int data_len);

    void remove(bloom_filter_id id);

   private:
    boost::interprocess::managed_shared_memory segment_;
    bf::id_bf_map *data_;
    bf::void_allocator get_allocator();
};

#endif