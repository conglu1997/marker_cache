#define BOOST_TEST_MODULE MarkerCacheTest
#include <markercache.h>
#include <boost/test/included/unit_test.hpp>
#include <vector>

using namespace std;

// Size constraints are now guaranteed by the creation of the marker cache

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
    friend class marker_cache;
    marker_cache* m;
    vector<pair<char*, int>> test_set_one;
    vector<pair<char*, int>> test_set_two;
    static const size_t test_size = 100000;
    static const double test_fprate;

    static const size_t dur = 30;
    static const size_t lifespan = 90;

    GeneratedMarkerCache() {
        BOOST_TEST_MESSAGE("Setup cache in shared memory");

        size_t num_filters = ceil((double)lifespan / (double)dur) + 1;

        m = new marker_cache(dur, lifespan, test_fprate,
                             test_size * num_filters);

        size_t min_test_width = 50;
        size_t max_test_width = 250;

        test_set_one =
            generate_test_data(test_size, min_test_width, max_test_width);
        test_set_two =
            generate_test_data(test_size, min_test_width, max_test_width);
    }

    ~GeneratedMarkerCache() {
        BOOST_TEST_MESSAGE("Destroy generated data");
        for (vector<pair<char*, int>>::iterator i = test_set_one.begin();
             i != test_set_one.end(); ++i)
            delete i->first;
        for (vector<pair<char*, int>>::iterator i = test_set_two.begin();
             i != test_set_two.end(); ++i)
            delete i->first;
        delete m;
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
};

const double GeneratedMarkerCache::test_fprate = 0.001;

BOOST_FIXTURE_TEST_SUITE(MarkerCacheTests, GeneratedMarkerCache)

BOOST_AUTO_TEST_CASE(NoFalseNegatives) {
    BOOST_REQUIRE(sizeof(char) == 1);  // Check chars are correct size

    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i) {
        BOOST_CHECK_NO_THROW(m->insert(i->first, i->second));
        BOOST_CHECK_MESSAGE(m->lookup_from_current(i->first, i->second),
                            "False Negative - fatal error");
    }
}

BOOST_AUTO_TEST_CASE(FalsePositiveRate) {
    BOOST_REQUIRE(sizeof(char) == 1);  // Check chars are correct size

    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i)
        BOOST_CHECK_NO_THROW(m->insert(i->first, i->second));

    size_t falsepos = 0;

    for (vector<pair<char*, int>>::const_iterator i = test_set_two.cbegin();
         i != test_set_two.cend(); ++i)
        if (m->lookup_from_current(i->first, i->second)) ++falsepos;

    double observed_fprate = (double)falsepos / (double)test_size;

    // This test needs a high sample size to be accurate
    BOOST_CHECK_CLOSE(test_fprate, observed_fprate, 30);
}

BOOST_AUTO_TEST_CASE(Ageing) {
    size_t num_filters = ceil((double)lifespan / (double)dur) + 1;

    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i) {
        BOOST_CHECK_NO_THROW(m->insert(i->first, i->second));
        BOOST_CHECK_MESSAGE(m->lookup_from_current(i->first, i->second),
                            "False Negative - fatal error");
    }

    for (int i = 0; i < num_filters - 1; ++i) {
        m->age();
        // Search entire timespan to make sure data still exists
        for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
             i != test_set_one.cend(); ++i)
            BOOST_CHECK_NO_THROW(m->lookup_from(
                0, (numeric_limits<time_t>::max)(), i->first, i->second));
    }
    m->age();
    // Ensure data is gone after *num_filters* ageing cycles
    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i)
        BOOST_CHECK_NO_THROW(!m->lookup_from(0, (numeric_limits<time_t>::max)(),
                                             i->first, i->second));
}

BOOST_AUTO_TEST_CASE(TimerangeLookups) {
    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i) {
        BOOST_CHECK_NO_THROW(m->insert(i->first, i->second));
        BOOST_CHECK_MESSAGE(m->lookup_from_current(i->first, i->second),
                            "False Negative - fatal error");
    }

    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i) {
        BOOST_CHECK_NO_THROW(m->lookup_from(
            time(NULL), (numeric_limits<time_t>::max)(), i->first, i->second));
        // Test data did not exist before current period
        BOOST_CHECK_NO_THROW(
            !m->lookup_from(0, time(NULL) - 100, i->first, i->second));
    }
}

BOOST_AUTO_TEST_SUITE_END()