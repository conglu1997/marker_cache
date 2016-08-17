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
    vector<pair<char*, int>> test_set_one;
    vector<pair<char*, int>> test_set_two;
    const static size_t test_size = 100000;
    const static int test_id = 1;

    GeneratedMarkerCache() : m(100000000) {
        BOOST_TEST_MESSAGE("Setup cache in shared memory");

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

BOOST_AUTO_TEST_CASE(NoFalseNegatives) {
    BOOST_REQUIRE(sizeof(char) == 1);  // Check chars are correct size

    BOOST_CHECK_NO_THROW(m.create(test_id, 0.001, test_size));

    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i) {
        BOOST_CHECK_NO_THROW(m.insert_into(test_id, i->first, i->second));
        BOOST_CHECK_MESSAGE(m.lookup_from(test_id, i->first, i->second),
                            "False Negative - fatal error");
    }
}

BOOST_AUTO_TEST_CASE(FalsePositiveRate) {
    BOOST_REQUIRE(sizeof(char) == 1);  // Check chars are correct size

    double test_fprate = 0.001;

    BOOST_CHECK_NO_THROW(m.create(test_id, test_fprate, test_size));

    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i)
        BOOST_CHECK_NO_THROW(m.insert_into(test_id, i->first, i->second));

    size_t falsepos = 0;

    for (vector<pair<char*, int>>::const_iterator i = test_set_two.cbegin();
         i != test_set_two.cend(); ++i)
        if (m.lookup_from(test_id, i->first, i->second)) ++falsepos;

    double observed_fprate = (double)falsepos / (double)test_size;

    BOOST_CHECK_CLOSE(test_fprate, observed_fprate,
                      10);  // Within 10% of each other
}

BOOST_AUTO_TEST_CASE(Creation) {
    double fprate = 0.5;
    size_t capacity = 1000;

    for (int i = 0; i < 100; ++i) {
        BOOST_CHECK_NO_THROW(m.create(i, fprate, capacity));
        BOOST_CHECK(m.exists(i));
        BOOST_CHECK(m.create(i, fprate, capacity) == 0);  // No double creation
    }
}

BOOST_AUTO_TEST_CASE(SizeConstraints) {
    double test_fprate = 0.001;

    double size1 = m.create(1, test_fprate, 1000000);
    double size2 = m.create(2, test_fprate, 2000000);
    double size3 = m.create(3, test_fprate, 3000000);

    // Consistent linear sizing within 1%
    BOOST_CHECK_CLOSE(size2 - size1, size3 - size2, 1);

    // ~4MB for 10 hash function overhead on 64-bit systems within 1%
    BOOST_CHECK_CLOSE(2 * size1 - size2, 4096128, 1);

    // 1-bit per bool (1m calc http://hur.st/bloomfilter?n=1000000&p=0.001)
    // 1% leeway
    BOOST_CHECK_CLOSE(1797198.5, size2 - size1, 1);
    BOOST_CHECK_CLOSE(1797198.5, size3 - size2, 1);
}

BOOST_AUTO_TEST_CASE(Removal) {
    double test_fprate = 0.5;
    size_t capacity = 1000;

    for (int i = 0; i < 100; ++i) {
        BOOST_CHECK_NO_THROW(m.remove(i));  // Empty removes
        BOOST_CHECK_NO_THROW(m.create(i, test_fprate, capacity));
        BOOST_CHECK(m.exists(i));
        BOOST_CHECK_NO_THROW(m.remove(i));
        BOOST_CHECK(m.exists(i) == false);
    }
}

BOOST_AUTO_TEST_CASE(Erasure) {
    double test_fprate = 0.5;
    size_t capacity = 1000;

    for (int i = 0; i < 100; ++i) {
        BOOST_CHECK_NO_THROW(m.create(i, test_fprate, capacity));
        BOOST_CHECK(m.exists(i));
    }

    BOOST_CHECK_NO_THROW(m.erase());

    for (int i = 0; i < 100; ++i) BOOST_CHECK(m.exists(i) == false);
}

BOOST_AUTO_TEST_CASE(QueryingFromNonExistant) {
    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i)
        BOOST_CHECK_MESSAGE(
            m.lookup_from(test_id, i->first, i->second) == false,
            "False negatives from non-existant bloom filter");
}

BOOST_AUTO_TEST_CASE(InsertingIntoNonExistant) {
    for (vector<pair<char*, int>>::const_iterator i = test_set_one.cbegin();
         i != test_set_one.cend(); ++i)
        BOOST_CHECK_MESSAGE(
            m.insert_into(test_id, i->first, i->second) == false,
            "Inserting into non-existant bloom filter");
}

BOOST_AUTO_TEST_SUITE_END()