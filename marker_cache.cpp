#include "marker_cache.h"

marker_cache::marker_cache(size_t bytes, boost::interprocess::create_only_t c)
    : segment_(c, "BFSharedMemory", bytes), owner_(true) {
    data_ = segment_.construct<id_bf_map>("MarkerCache")(
        std::less<int>(), get_allocator());
    assert(segment_.find<bf::id_bf_map>("MarkerCache").first != 0);
}

marker_cache::marker_cache(size_t bytes, boost::interprocess::open_read_only_t o)
	: segment_(o, "BFSharedMemory"), owner_(false) {
	data_ = segment_.find<id_bf_map>("MarkerCache").first;
	assert(data_ != NULL);
}

marker_cache::~marker_cache() {
    // Clear shared memory on exit if we own the memory
    if (owner_) {
        boost::interprocess::shared_memory_object::remove("BFSharedMemory");
    }
}

size_t marker_cache::create(const marker_cache_id id, double fp,
                            size_t capacity, size_t seed, bool double_hashing,
                            bool partition) {
	// Throws if a read-only trys to create a bloom filter
    auto f = segment_.get_free_memory();
    data_->insert(
        bf_pair(id, bf::shm_bloom_filter(get_allocator(), fp, capacity, seed,
                                         double_hashing, partition)));
    return f - segment_.get_free_memory();
}

bool marker_cache::exists(marker_cache_id id) {
    return data_->find(id) != data_->end();
}

bool marker_cache::lookup_from(marker_cache_id id, char *data,
                               int data_len) const {
    return data_->at(id).lookup(data, data_len);
    // std::out_of_range thrown if invalid access
}

void marker_cache::insert_into(marker_cache_id id, char *data, int data_len) {
    data_->at(id).add(data, data_len);
    // std::out_of_range thrown if invalid access
}

void marker_cache::remove(marker_cache_id id) {
    data_->erase(id);
    // may throw an exception from the compare object
}

bf::void_allocator marker_cache::get_allocator() {
    return segment_.get_segment_manager();
}
