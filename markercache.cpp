#include <markercache.h>

marker_cache::marker_cache(size_t min_filterduration, size_t min_filterlifespan,
                           double fp, size_t total_capacity)
    : owner_(true), sec_filterduration(60 * min_filterduration) {
    // Clear shared memory object if it exists before creation
    boost::interprocess::shared_memory_object::remove("CacheSharedMemory");

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

    // Give 10KB per filter for deque overhead and padding
    segment_ = new boost::interprocess::managed_shared_memory(
        boost::interprocess::create_only, "CacheSharedMemory",
        m + num_filters * 10000);
    buf_ = segment_->construct<cache_buffer>("MarkerCache")(get_allocator());
    assert(segment_->find<cache_buffer>("MarkerCache").first != NULL);

    mutex = segment_->find_or_construct<
        boost::interprocess::interprocess_sharable_mutex>("CacheMutex")();

    size_t filter_capacity = std::ceil((double)m / (double)num_filters);
    time_t now = time(NULL);

    // NO FILTERS DETECTED... calculate filler dates
    // We create many empty filters because we want to fix the number of filters
    // at the start and recycle old ones as appropriate since we allocate a
    // fixed amount of memory
    // TODO: Let the code read Bloom filters from disk and then calculate dates
    // from that
    time_t start = now -= sec_filterduration * (num_filters - 1);
    // Setup the memory at the start
    for (size_t i = 0; i < num_filters; ++i) {
        buf_->push_back(
            bf_pair(timerange(start, start + sec_filterduration - 1),
                    bf::shm_bloom_filter(get_allocator(), filter_capacity, k)));
        start += sec_filterduration;
    }
    // Mark the current filter
    buf_->back().first.second = (std::numeric_limits<time_t>::max)();
}

marker_cache::marker_cache() : owner_(false) {
    segment_ = new boost::interprocess::managed_shared_memory(
        boost::interprocess::open_read_only, "CacheSharedMemory");
    buf_ = segment_->find<cache_buffer>("MarkerCache").first;
    assert(buf_ != NULL);
    mutex = segment_->find_or_construct<
        boost::interprocess::interprocess_sharable_mutex>("CacheMutex")();
}

marker_cache::~marker_cache() {
    if (owner_)
        boost::interprocess::shared_memory_object::remove("CacheSharedMemory");

    delete segment_;
}

bool marker_cache::lookup_from(time_t start, time_t end, char* data,
                               int data_len) const {
    // Invalid timerange
    if (start > end) return false;
    // Reference to deleted data
    if (end < buf_->front().first.first) return false;

    // Hash once for the full iteration
    hash128_t h = bf::shm_bloom_filter::hash(data, data_len);

    timerange search_period = timerange(start, end);
    bool within_search_period = false;

    // Allow multiple threads from SD to lookup data
    boost::interprocess::sharable_lock<
        boost::interprocess::interprocess_sharable_mutex>
        lock(*mutex);

    // Iterate through the buffer, searching in the overlapping timerange;
    for (cache_buffer::iterator i = buf_->begin(); i != buf_->end(); ++i) {
        if (!overlapping_timerange(search_period, i->first)) {
            if (!within_search_period) {
                continue;
            } else {
                break;
            }
        }
        within_search_period = true;
        if (i->second.lookup(h)) return true;
    }

    return false;
}

void marker_cache::insert(char* data, int data_len) {
    // Note: We do not need to acquire a lock while inserting since ageing will
    // not invalidate references to data that was not deleted
    buf_->back().second.insert(bf::shm_bloom_filter::hash(data, data_len));
}

void marker_cache::maybe_age(bool force) {
    if (force || (buf_->back().first.first + sec_filterduration <= time(NULL)))
        age();
}

void marker_cache::age() {
    // Forbid searching while ageing the data since removing elements will
    // invalidate the cache_buffer iterators
    boost::interprocess::scoped_lock<
        boost::interprocess::interprocess_sharable_mutex>
        lock(*mutex);
    time_t now = time(NULL);
    // Set finishing time for the current filter
    buf_->back().first.second = std::max(now, buf_->back().first.first);

    // Move the oldest filter to the front and reset it
    bf::shm_bloom_filter tmp = buf_->front().second;
    buf_->pop_front();
    tmp.reset();
    buf_->push_back(
        bf_pair(timerange(now + 1, (std::numeric_limits<time_t>::max)()), tmp));
}

bf::void_allocator marker_cache::get_allocator() {
    return segment_->get_segment_manager();
}

bool marker_cache::overlapping_timerange(timerange fst, timerange snd) const {
    // Assume ranges are valid
    return (fst.first <= snd.second) && (snd.first <= fst.second);
}