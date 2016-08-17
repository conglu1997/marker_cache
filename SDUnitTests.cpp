#define BOOST_TEST_MODULE MarkerCacheTest
#include <markercache.h>
#include <boost/test/included/unit_test.hpp>
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
    const static size_t test_size = 100000;

    GeneratedMarkerCache() : m() {
        BOOST_TEST_MESSAGE("Read existing shared memory");

        size_t min_test_width = 50;
        size_t max_test_width = 250;

        test_set =
            generate_test_data(test_size, min_test_width, max_test_width);
    }

    ~GeneratedMarkerCache() {
        BOOST_TEST_MESSAGE("Destroy generated data");
        for (vector<pair<char*, int>>::iterator i = test_set.begin();
             i != test_set.end(); ++i)
            delete i->first;
    }

    // Generate length-8 random strings for testing
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

BOOST_FIXTURE_TEST_SUITE(MarkerCacheTests, GeneratedMarkerCache)

/*
        Testing the shared memory is in fact read-only
                Can't create, insert, remove, erase, reset
        Testing the same filters exist on the search director side
                Exists
        Testing reads and false positives
*/

BOOST_AUTO_TEST_CASE(ReadOnly) {
    for (vector<pair<char*, int>>::const_iterator i = test_set.cbegin();
         i != test_set.cend(); ++i)
        BOOST_CHECK_NO_THROW(m.insert_into(1, i->first, i->second));
}

BOOST_AUTO_TEST_CASE(Existence) { BOOST_CHECK(m.exists(1)); }

BOOST_AUTO_TEST_CASE(Readable) {
    size_t found = 0;

    for (vector<pair<char*, int>>::const_iterator i = test_set.cbegin();
         i != test_set.cend(); ++i)
        if (m.insert_into(1, i->first, i->second)) found++;

    BOOST_CHECK(found > 0, "Should be false positives");
}

BOOST_AUTO_TEST_SUITE_END()