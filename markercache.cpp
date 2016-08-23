#include <markercache.h>

// Need to be careful about what happens to data that's located at the cutoff
// point between two filters and that we don't lose any data at this point

marker_cache::marker_cache(size_t min_filterduration, size_t min_filterlifespan,
                           double fp, size_t total_capacity)
    : owner_(true), sec_filterduration(60 * min_filterduration) {
    // Clear shared memory object if it exists before creation
    boost::interprocess::shared_memory_object::remove("BFSharedMemory");

    // Work out optimum parameters
    double ln2 = std::log(2);
    // Num. bits - https://en.wikipedia.org/wiki/Bloom_filter
    // m = -(nln(p))/(ln2^2) where n = num objects, p = false pos rate
    size_t m = std::ceil(-(((total_capacity * std::log(fp)) / ln2) / ln2));
    double frac = (double)m / (double)total_capacity;
    // Num. hash functions
    // k = (m/n)*ln2
    size_t k = std::ceil(frac * ln2);

    size_t num_filters =
        std::ceil((double)min_filterlifespan / (double)min_filterduration) + 1;

    // Give 10KB overhead per filter
    segment_ = new boost::interprocess::managed_shared_memory(
        boost::interprocess::create_only, "BFSharedMemory",
        m + num_filters * 10000);

    data_ = segment_->construct<cache_buffer>("MarkerCache")(get_allocator());
    assert(segment_->find<cache_buffer>("MarkerCache").first != NULL);

    size_t filter_capacity = std::ceil((double)m / (double)num_filters);
    time_t now = time(NULL);

    // NO FILTERS DETECTED... calculate filler dates
    // TODO: Let the code read Bloom filters from disk and then calculate dates
    // from that
    time_t start = now -= sec_filterduration * (num_filters - 1);
    // Setup the memory at the start
    for (size_t i = 0; i < num_filters; ++i) {
        data_->push_back(
            bf_pair(timerange(start, start + sec_filterduration - 1),
                    bf::shm_bloom_filter(get_allocator(), filter_capacity, k)));
        // Move forwards by the duration
        start += sec_filterduration;
    }
    // Mark the current filter
    data_->back().first.second = (std::numeric_limits<time_t>::max)();
}

marker_cache::marker_cache() : owner_(false) {
    segment_ = new boost::interprocess::managed_shared_memory(
        boost::interprocess::open_read_only, "BFSharedMemory");
    data_ = segment_->find<cache_buffer>("MarkerCache").first;
    assert(data_ != NULL);
}

marker_cache::~marker_cache() {
    if (owner_)
        boost::interprocess::shared_memory_object::remove("BFSharedMemory");

    delete segment_;
}

bool marker_cache::lookup_from(time_t start, time_t end, char *data,
                               int data_len) const {
    // Invalid timerange
    if (start > end) return false;
    // Reference to deleted data
    if (end < data_->front().first.first) return false;

    // Assume searches tend to be closer to the current, do naive linear search
    // for now (In case we have odd cache lengths)
    cache_buffer::iterator end_it = --data_->end();
    while (!within_timerange(end, end_it->first) && end_it != data_->begin())
        end_it--;

    cache_buffer::iterator start_it = end_it;
    while (!within_timerange(start, start_it->first) &&
           start_it != data_->begin())
        start_it--;

    // Hash once for the full iteration
    uint64_t *h = bf::shm_bloom_filter::hash(data, data_len);

    for (; start_it != end_it + 1; ++start_it) {
        if (start_it->second.lookup(h)) {
            delete[] h;
            return true;
        }
    }
    delete[] h;
    return false;
}

bool marker_cache::lookup_from_current(char *data, int data_len) const {
    return lookup_from((std::numeric_limits<time_t>::max)(),
                       (std::numeric_limits<time_t>::max)(), data, data_len);
}

void marker_cache::insert(char *data, int data_len) {
    uint64_t *h = bf::shm_bloom_filter::hash(data, data_len);
    data_->back().second.insert(h);
    delete[] h;
}

void marker_cache::maybe_age() {
    if (data_->back().first.first + sec_filterduration <= time(NULL)) age();
}

void marker_cache::age() {
    time_t now = time(NULL);
    // Set finishing time for the current filter
    data_->back().first.second = std::max(now, data_->back().first.first);

    // Move the oldest filter to the front and reset it
    bf::shm_bloom_filter tmp = data_->front().second;
    data_->pop_front();
    tmp.reset();
    data_->push_back(
        bf_pair(timerange(now + 1, (std::numeric_limits<time_t>::max)()), tmp));
}

bf::void_allocator marker_cache::get_allocator() {
    return segment_->get_segment_manager();
}

bool marker_cache::within_timerange(time_t time, timerange range) const {
    // Inclusive timeranges
    return (time >= range.first) && (time <= range.second);
}
