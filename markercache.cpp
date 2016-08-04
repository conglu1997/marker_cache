#include "markercache.h"

marker_cache::marker_cache(size_t bytes)
    : segment_(boost::interprocess::create_only, "BFSharedMemory", bytes),
      owner_(true) {
    data_ = segment_.construct<id_bf_map>("MarkerCache")(std::less<int>(),
                                                         get_allocator());
    assert(segment_.find<bf::id_bf_map>("MarkerCache").first != NULL);
}

marker_cache::marker_cache()
    : segment_(boost::interprocess::open_read_only, "BFSharedMemory"),
      owner_(false) {
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
                            size_t capacity, size_t seed) {
    // Throws if a read-only trys to create a bloom filter
    auto f = segment_.get_free_memory();

    // Work out optimum parameters
    auto ln2 = std::log(2);
    // Num. bits - https://en.wikipedia.org/wiki/Bloom_filter
    // m = -(nln(p))/(ln2^2) where n = num objects, p = false pos rate
    size_t m = std::ceil(-(((capacity * std::log(fp)) / ln2) / ln2));
    auto frac = static_cast<double>(m) / static_cast<double>(capacity);
    // Num. hash functions
    // k = (m/n)*ln2
    size_t k = std::ceil(frac * ln2);

    data_->insert(
        bf_pair(id, bf::shm_bloom_filter(get_allocator(), m, k, seed)));
    return f - segment_.get_free_memory();
}

bool marker_cache::exists(marker_cache_id id) const {
    return data_->find(id) != data_->end();
}

bool marker_cache::lookup_from(marker_cache_id id, char *data,
                               int data_len) const {
    if (exists(id)) {
        return data_->at(id).lookup(data, data_len);
    } else {
        return false;
    }
}

bool marker_cache::insert_into(marker_cache_id id, char *data, int data_len) {
    if (exists(id)) {
        data_->at(id).insert(data, data_len);
        return true;
    } else {
        return false;
    }
}

void marker_cache::remove(marker_cache_id id) {
    if (exists(id)) data_->erase(id);
}

void marker_cache::erase() {
    for (auto it = data_->cbegin(); it != data_->cend();) {
        data_->erase(it++);
    }
}

bf::void_allocator marker_cache::get_allocator() {
    return segment_.get_segment_manager();
}