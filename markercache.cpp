#include <markercache.h>

marker_cache::marker_cache(size_t min_filterduration, size_t min_filterlifespan,
                           double fp, size_t total_capacity)
    : owner_(true), sec_filterduration(60 * min_filterduration) {
    assert(min_filterduration > 0);
    assert(min_filterlifespan > 0);

    boost::log::add_file_log(
        boost::log::keywords::file_name = "cache_%N.log",
        boost::log::keywords::rotation_size = 10 * 1024 * 1024,
        boost::log::keywords::format = "[%TimeStamp%]: %Message%",
        boost::log::keywords::open_mode = std::ios_base::app);
    boost::log::add_common_attributes();

    // Set the directory for writing Bloom filters
    archive_dir = "archive";

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
    BOOST_LOG_SEV(lg, boost::log::trivial::info)
        << "New cache instantiated with " << m + num_filters * 10000
        << " bytes.";
    segment_ = new boost::interprocess::managed_shared_memory(
        boost::interprocess::create_only, "CacheSharedMemory",
        m + num_filters * 10000);
    buf_ = segment_->construct<cache_buffer>("MarkerCache")(get_allocator());
    assert(segment_->find<cache_buffer>("MarkerCache").first != NULL);

    mutex = segment_->find_or_construct<
        boost::interprocess::interprocess_sharable_mutex>("CacheMutex")();

    size_t filter_capacity = std::ceil((double)m / (double)num_filters);

    std::vector<boost::filesystem::path> v;
    time_t now = time(NULL);

    if (boost::filesystem::exists(archive_dir) &&
        boost::filesystem::is_directory(archive_dir)) {
        boost::filesystem::directory_iterator it(archive_dir);

        // Load list of saved filters
        while (it != boost::filesystem::directory_iterator()) {
            if (it->path().extension() == ".filter") {
                // Extract the timestamp and ignore outdated filters
                if (std::stoi(
                        it->path().filename().replace_extension("").string()) +
                        sec_filterduration * num_filters >=
                    now) {
                    // Active filter
                    v.push_back(it->path());
                } else {
                    // Clear the disk
                    boost::filesystem::remove(it->path());
                }
            }
            ++it;
        }
    }

    // Load any detected filters into the buffer
    std::sort(v.begin(), v.end());
    BOOST_LOG_SEV(lg, boost::log::trivial::trace)
        << "Attempting to load filters from disk:";
    // Start with the latest Bloom filters
    for (std::vector<boost::filesystem::path>::reverse_iterator i = v.rbegin();
         i != v.rend(); ++i) {
        // Stop loading if capacity reached, reserve space for current filter
        if (buf_->size() >= num_filters - 1) break;

        std::ifstream ifs(i->string());
        boost::archive::text_iarchive ia(ifs);
        bf_pair b(get_allocator());
        ia >> b;
        BOOST_LOG_SEV(lg, boost::log::trivial::info)
            << "Loaded filter: " << b.first.first << " -> " << b.first.second;
        buf_->push_front(b);
    }
    BOOST_LOG_SEV(lg, boost::log::trivial::trace) << "Finished loading.";

    if (buf_->empty()) {
        // No filters loaded
        BOOST_LOG_SEV(lg, boost::log::trivial::info) << "New filter at: "
                                                     << now;
        buf_->push_back(
            bf_pair(timerange(now, (std::numeric_limits<time_t>::max)()),
                    bf::shm_bloom_filter(get_allocator(), filter_capacity, k)));
    } else {
        // Resume the filter from the last stopping point
        // Query the database for markers that lie in the missing timerange
        while (buf_->back().first.second <= now) {
            time_t rebuild_start = buf_->back().first.second;
            time_t rebuild_end = rebuild_start + sec_filterduration - 1;
            BOOST_LOG_SEV(lg, boost::log::trivial::info)
                << "Rebuilding filter from: " << rebuild_start << " to "
                << rebuild_end;
            buf_->push_back(bf_pair(
                timerange(std::max(buf_->back().first.first + 1, rebuild_start),
                          rebuild_end),
                bf::shm_bloom_filter(get_allocator(), filter_capacity, k)));

            // Query the database between the two end points
            std::vector<std::pair<char*, int>> queried_markers;

            /* TODO: Insert magic querying code here */

            for (std::vector<std::pair<char*, int>>::iterator i =
                     queried_markers.begin();
                 i != queried_markers.end(); ++i)
                insert(i->first, i->second);

            // Guarantee the number of filters does not exceed capacity
            maybe_age();
        }
    }

    // Mark the current filter
    buf_->back().first.second = (std::numeric_limits<time_t>::max)();

    // Backdate empty filters to allow ageing cycles
    while (buf_->size() < num_filters)
        buf_->push_front(
            bf_pair(timerange(buf_->front().first.first - sec_filterduration,
                              buf_->front().first.first - 1),
                    bf::shm_bloom_filter(get_allocator(), filter_capacity, k)));
}

marker_cache::marker_cache() : owner_(false) {
    // The reading process needs to be able to lock the mutex
    segment_ = new boost::interprocess::managed_shared_memory(
        boost::interprocess::open_only, "CacheSharedMemory");
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

    // Iterate through the buffer, searching in the overlapping timerange
    // Searches are more likely to be on recent data, start from the end
    for (cache_buffer::reverse_iterator i = buf_->rbegin(); i != buf_->rend();
         ++i) {
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
    if (force ||
        (buf_->back().first.first + sec_filterduration <= time(NULL))) {
        // Forbid searching while ageing the data since removing elements will
        // invalidate the cache_buffer iterators
        BOOST_LOG_SEV(lg, boost::log::trivial::trace)
            << "Started an ageing cycle: ";
        boost::interprocess::scoped_lock<
            boost::interprocess::interprocess_sharable_mutex>
            lock(*mutex);
        time_t now = time(NULL);
        // Set finishing time for the current filter
        buf_->back().first.second = std::max(now, buf_->back().first.first);

        // Delete the outdated filter, only keep active filters on disk
        BOOST_LOG_SEV(lg, boost::log::trivial::info)
            << "Cleared filter: " << buf_->front().first.first;
        boost::filesystem::path path =
            timestamp_to_filepath(buf_->front().first.first);
        boost::filesystem::remove(path);

        // Move the oldest filter to the front and reset it
        bf::shm_bloom_filter tmp = buf_->front().second;
        buf_->pop_front();
        tmp.reset();
        // Enforce unique starting points for the filters
        buf_->push_back(bf_pair(timerange(buf_->back().first.second + 1,
                                          (std::numeric_limits<time_t>::max)()),
                                tmp));
        BOOST_LOG_SEV(lg, boost::log::trivial::info)
            << "New filter at: " << buf_->back().first.first;

        save();
        BOOST_LOG_SEV(lg, boost::log::trivial::trace)
            << "Ended an ageing cycle: ";
    }
}

bf::void_allocator marker_cache::get_allocator() {
    return segment_->get_segment_manager();
}

void marker_cache::save() {
    if (!boost::filesystem::exists(archive_dir))
        boost::filesystem::create_directory(archive_dir);

    BOOST_LOG_SEV(lg, boost::log::trivial::trace) << "Starting saving cycle:";
    // During serialization, allocator information is stripped away.
    for (cache_buffer::iterator i = buf_->begin(); i != buf_->end(); ++i) {
        // Label the file with the starting timestamp
        boost::filesystem::path path = timestamp_to_filepath(i->first.first);

        if (i->first.second != (std::numeric_limits<time_t>::max)() &&
            !boost::filesystem::exists(path)) {
            // Write the filter if it's not already written and is not current
            BOOST_LOG_SEV(lg, boost::log::trivial::info) << "Writing to: "
                                                         << path;
            std::ofstream ofs(path.string());
            boost::archive::text_oarchive oa(ofs);
            oa << *i;
        }
    }
    BOOST_LOG_SEV(lg, boost::log::trivial::trace) << "Finished saving.";
}

bool marker_cache::overlapping_timerange(timerange fst, timerange snd) const {
    // Assume ranges are valid
    return (fst.first <= snd.second) && (snd.first <= fst.second);
}

boost::filesystem::path marker_cache::timestamp_to_filepath(time_t t) {
    std::ostringstream ss;
    ss << archive_dir.string() << '/' << t << ".filter";
    return ss.str();
}
