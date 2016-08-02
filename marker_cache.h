#ifndef MARKER_CACHE_H
#define MARKER_CACHE_H
#include "shm_bloom_filter.h"

class marker_cache {
    // API for managing shared memory and retrieving handles to data
    // Takes identifier of the bloom filter (per subtable, per marker type)
    // For our purposes, identifier is just the marker type (int32)
   public:
    // Create new cache, size bits, fp false-positive rate
    marker_cache(size_t bytes);
    ~marker_cache();
    void create(int id, double fp, size_t capacity, size_t seed = 0,
                bool double_hashing = true, bool partition = true);

	// data_len is num of chars (bytes)
    bool lookup_from(int id, char *data, int data_len) const;

    void insert_into(int id, char *data, int data_len);

    void remove(int id);

   private:
    boost::interprocess::managed_shared_memory segment_;
    bf::id_bf_map *data_;
    bf::void_allocator get_allocator();
};

#endif