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
    vector<pair<char *, int>> test_set;
    const static size_t test_size = 1000000;
    const static size_t min_test_width = 50;
    const static size_t max_test_width = 250;

    GeneratedMarkerCache() : m() {
        BOOST_TEST_MESSAGE("Read existing shared memory");

        test_set =
            generate_test_data(test_size, min_test_width, max_test_width);
    }

    ~GeneratedMarkerCache() {
        BOOST_TEST_MESSAGE("Destroy generated data");
        for (vector<pair<char *, int>>::iterator i = test_set.begin();
             i != test_set.end(); ++i)
            delete i->first;
    }

    // Generate length-8 random strings for testing
    vector<pair<char *, int>> generate_test_data(size_t num_elems,
                                                 size_t min_width,
                                                 size_t max_width) {
        vector<pair<char *, int>> v;
        string chars(
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "1234567890"
            "!@#$%^&*()"
            "`~-_=+[{]}\\|;:'\",<.>/? ");

        size_t width = random(min_width, max_width);

        for (size_t i = 0; i < num_elems; ++i) {
            char *s = new char[width];
            for (size_t i = 0; i < width; ++i)
                s[i] = chars[random(0, chars.size() - 1)];
            v.push_back(pair<char *, int>(s, width));
        }

        return v;
    }
};

BOOST_FIXTURE_TEST_SUITE(MarkerCacheTests, GeneratedMarkerCache)

/*
Testing the shared memory is in fact read-only
--Throws memory access violation if write attempted
--Not included since it's not a language level exception
Testing the same filters exist on the search director side
Testing reads and false positives
*/

BOOST_AUTO_TEST_CASE(Existence) {
    BOOST_CHECK_MESSAGE(m.exists(1), "Cannot find bloom filter");
}

BOOST_AUTO_TEST_CASE(Readable) {
    size_t found = 0;
    clock_t t1, t2;

    t1 = clock();

    for (vector<pair<char *, int>>::const_iterator i = test_set.cbegin();
         i != test_set.cend(); ++i)
        if (m.lookup_from(1, i->first, i->second)) found++;

    t2 = clock();

    float t(((float)t2 - (float)t1) / CLOCKS_PER_SEC);

    cout << found << " false positives found out of " << test_size << "."
         << endl;
    cout << "Completed " << test_size << " searches of average length "
         << (min_test_width + max_test_width) / 2 << " bytes in " << t
         << " seconds." << endl;
    BOOST_CHECK_MESSAGE(found > 0, "Should be false positives");
}

BOOST_AUTO_TEST_SUITE_END()