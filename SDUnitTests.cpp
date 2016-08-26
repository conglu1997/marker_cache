#define BOOST_TEST_MODULE MarkerCacheTest
#include <markercache.h>
#include <boost/test/included/unit_test.hpp>
#include <ctime>
#include <iostream>
#include <vector>

using namespace std;

size_t random(size_t min, size_t max)  // range : [min, max]
{
    static bool first = true;
    if (first) {
        srand(time(NULL));  // seeding for the first time only!
        first = false;
    }
    return min + rand() % (max - min + 1);
}

struct GeneratedMarkerCache {
    marker_cache m;
    vector<pair<char*, int>> test_set;
    static const size_t test_size = 100000;

    const static size_t min_test_width = 50;
    const static size_t max_test_width = 250;

    GeneratedMarkerCache() : m() {
        BOOST_TEST_MESSAGE("Read existing cache in shared memory");

        test_set =
            generate_test_data(test_size, min_test_width, max_test_width);
    }

    ~GeneratedMarkerCache() {
        BOOST_TEST_MESSAGE("Destroy generated data");
        for (vector<pair<char*, int>>::iterator i = test_set.begin();
             i != test_set.end(); ++i)
            delete i->first;
    }

    vector<pair<char*, int>> generate_test_data(size_t num_elems,
                                                size_t min_width,
                                                size_t max_width) {
        vector<pair<char*, int>> v;
        string chars(
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "1234567890"
            "!@#$%^&*()"
            "`~-_=+[{]}\\|;:'\",<.>/? ");

        size_t width = random(min_width, max_width);

        for (size_t i = 0; i < num_elems; ++i) {
            char* s = new char[width];
            for (size_t i = 0; i < width; ++i)
                s[i] = chars[random(0, chars.size() - 1)];
            v.push_back(pair<char*, int>(s, width));
        }

        return v;
    }

    bool lookup_from_current(char* data, int data_len) const {
        return m.lookup_from((std::numeric_limits<time_t>::max)(),
                             (std::numeric_limits<time_t>::max)(), data,
                             data_len);
    }

    bool lookup_from_all(char* data, int data_len) const {
        return m.lookup_from(0, (std::numeric_limits<time_t>::max)(), data,
                             data_len);
    }
};

BOOST_FIXTURE_TEST_SUITE(MarkerCacheTests, GeneratedMarkerCache)

BOOST_AUTO_TEST_CASE(Readable) {
    size_t num_found = 0;
    clock_t t1, t2;

    t1 = clock();

    for (vector<pair<char*, int>>::const_iterator i = test_set.cbegin();
         i != test_set.cend(); ++i) {
        BOOST_CHECK_NO_THROW(lookup_from_current(i->first, i->second));
        if (lookup_from_current(i->first, i->second)) num_found++;
    }

    t2 = clock();

    float t(((float)t2 - (float)t1) / CLOCKS_PER_SEC);

    cout << num_found << " false positives found out of " << test_size << "."
         << endl;
    cout << "Completed " << test_size << " searches of average length "
         << (min_test_width + max_test_width) / 2 << " bytes in " << t
         << " seconds (current filter)." << endl;
    BOOST_CHECK_MESSAGE(num_found > 0, "Should be false positives");
}

BOOST_AUTO_TEST_CASE(TimerangeSearch) {
    size_t num_found = 0;
    clock_t t1, t2;

    t1 = clock();

    for (vector<pair<char*, int>>::const_iterator i = test_set.cbegin();
         i != test_set.cend(); ++i) {
        BOOST_CHECK_NO_THROW(lookup_from_all(i->first, i->second));
        if (lookup_from_all(i->first, i->second)) num_found++;
    }

    t2 = clock();

    float t(((float)t2 - (float)t1) / CLOCKS_PER_SEC);

    cout << num_found << " false positives found out of " << test_size << "."
         << endl;
    cout << "Completed " << test_size << " searches of average length "
         << (min_test_width + max_test_width) / 2 << " bytes in " << t
         << " seconds (all filters)." << endl;
    BOOST_CHECK_MESSAGE(num_found > 0, "Should be false positives");
}

BOOST_AUTO_TEST_SUITE_END()