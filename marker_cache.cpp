#include "marker_cache.h"

marker_cache::marker_cache(size_t bytes)
    : segment_(boost::interprocess::create_only, "BFSharedMemory", bytes) {
    data_ = segment_.construct<bf::id_bf_map>("MarkerCache")(std::less<int>(),
                                                             get_allocator());
}

marker_cache::~marker_cache() {
    // Clear shared memory on exit
    boost::interprocess::shared_memory_object::remove("BFSharedMemory");
}

void marker_cache::create(const int id, double fp, size_t capacity, size_t seed,
                          bool double_hashing, bool partition) {
    data_->insert(
        bf::bf_pair(id, bf::shm_bloom_filter(get_allocator(), fp, capacity,
                                             seed, double_hashing, partition)));
}

bool marker_cache::lookup_from(int id, char *data, int data_len) const {
    return data_->at(id).lookup(data, data_len);
    // std::out_of_range thrown if invalid access
}

void marker_cache::insert_into(int id, char *data, int data_len) {
    data_->at(id).add(data, data_len);
    // std::out_of_range thrown if invalid access
}

void marker_cache::remove(int id) {
    data_->erase(id);
    // may throw an exception from the compare object
}

bf::void_allocator marker_cache::get_allocator() {
    return segment_.get_segment_manager();
}
