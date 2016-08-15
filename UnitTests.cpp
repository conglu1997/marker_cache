#define BOOST_TEST_MODULE MarkerCacheTest
#include <markercache.h>
#include <boost/test/included/unit_test.hpp>
#include <iostream>
#include <random>
#include <vector>

using namespace std;

struct GeneratedMarkerCache {
    marker_cache m;
    vector<pair<char*, int>> test_set_one;
    vector<pair<char*, int>> test_set_two;
    size_t test_size = 1000000;  // Set parameters for testing
    double test_fprate = 0.001;
    int test_id = 1;

    GeneratedMarkerCache() : m(54000000) {  // 5GB scaled down by 100
        BOOST_TEST_MESSAGE("Setup cache in shared memory");

        size_t min_test_width = 8;
        size_t max_test_width = 16;

        test_set_one =
            generate_test_data(test_size, min_test_width, max_test_width);
        test_set_two =
            generate_test_data(test_size, min_test_width, max_test_width);
    }

    ~GeneratedMarkerCache() {
        BOOST_TEST_MESSAGE("Destroy generated data");
        for (auto i = test_set_one.begin(); i != test_set_one.end(); ++i)
            delete i->first;
        for (auto i = test_set_two.begin(); i != test_set_two.end(); ++i)
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

        default_random_engine rng(random_device{}());
        uniform_int_distribution<size_t> distribution(0, chars.size() - 1);
        uniform_int_distribution<size_t> wdistribution(min_width, max_width);
        size_t width = wdistribution(rng);

        for (size_t i = 0; i < num_elems; ++i) {
            char* s = new char[width];
            for (size_t i = 0; i < width; ++i) s[i] = chars[distribution(rng)];
            v.push_back(pair<char*, int>(s, width));
        }

        return v;
    }
};

BOOST_FIXTURE_TEST_SUITE(MarkerCacheTests, GeneratedMarkerCache)

BOOST_AUTO_TEST_CASE(NoFalseNegatives) {
    BOOST_REQUIRE(sizeof(char) == 1);  // Check chars are correct size

    BOOST_CHECK_NO_THROW(m.create(test_id, test_fprate, test_size));

    for (auto i = test_set_one.begin(); i != test_set_one.end(); ++i) {
        BOOST_CHECK_NO_THROW(m.insert_into(test_id, i->first, i->second));
        BOOST_CHECK_MESSAGE(m.lookup_from(test_id, i->first, i->second),
                            "False Negative - fatal error");
    }
}

BOOST_AUTO_TEST_CASE(FalsePositiveRate) {
    BOOST_REQUIRE(sizeof(char) == 1);  // Check chars are correct size

    BOOST_CHECK_NO_THROW(m.create(test_id, test_fprate, test_size));

    for (auto i = test_set_one.begin(); i != test_set_one.end(); ++i)
        BOOST_CHECK_NO_THROW(m.insert_into(test_id, i->first, i->second));

    size_t falsepos = 0;

    for (auto i = test_set_two.cbegin(); i != test_set_two.cend(); ++i)
        if (m.lookup_from(test_id, i->first, i->second)) ++falsepos;

    auto observed_fprate = (double)falsepos / (double)test_size;

    BOOST_CHECK_CLOSE(test_fprate, observed_fprate,
                      10);  // Within 10% of each other
}

BOOST_AUTO_TEST_CASE(Creation) {
    auto fprate = 1;
    auto capacity = 1;

    for (auto i = 0; i < 1000; ++i) {
        BOOST_CHECK_NO_THROW(m.create(i, fprate, capacity));
        BOOST_CHECK(m.exists(i));
        BOOST_CHECK(m.create(i, fprate, capacity) == 0);  // No double creation
    }
}

BOOST_AUTO_TEST_CASE(SizeConstraints) {
    auto fprate = 0.001;

    auto size1 = static_cast<double>(m.create(1, fprate, 1000000));
    auto size2 = static_cast<double>(m.create(2, fprate, 2000000));
    auto size3 = static_cast<double>(m.create(3, fprate, 3000000));

    // Consistent linear sizing within 1%
    BOOST_CHECK_CLOSE(size2 - size1, size3 - size2, 1);

    // ~72KB hash function overhead on 64-bit systems within 1%
    BOOST_CHECK_CLOSE(2 * size1 - size2, 737408, 1);

    // 1-bit per bool (1m calc http://hur.st/bloomfilter?n=1000000&p=0.001)
    // 1% leeway
    BOOST_CHECK_CLOSE(1797198.5, size2 - size1, 1);
    BOOST_CHECK_CLOSE(1797198.5, size3 - size2, 1);
}

BOOST_AUTO_TEST_CASE(Removal) {
    auto fprate = 1;
    auto capacity = 1;

    for (auto i = 0; i < 1000; ++i) {
        BOOST_CHECK_NO_THROW(m.remove(i));  // Empty removes
        BOOST_CHECK_NO_THROW(m.create(i, fprate, capacity));
        BOOST_CHECK(m.exists(i));
        BOOST_CHECK_NO_THROW(m.remove(i));
        BOOST_CHECK(m.exists(i) == false);
    }
}

BOOST_AUTO_TEST_CASE(Erasure) {
    auto fprate = 1;
    auto capacity = 1;

    for (auto i = 0; i < 1000; ++i) {
        BOOST_CHECK_NO_THROW(m.create(i, fprate, capacity));
        BOOST_CHECK(m.exists(i));
    }

    BOOST_CHECK_NO_THROW(m.erase());

    for (auto i = 0; i < 1000; ++i) BOOST_CHECK(m.exists(i) == false);
}

BOOST_AUTO_TEST_CASE(QueryingFromNonExistant) {
    for (auto i = test_set_one.begin(); i != test_set_one.end(); ++i)
        BOOST_CHECK_MESSAGE(
            m.lookup_from(test_id, i->first, i->second) == false,
            "False negatives from non-existant bloom filter");
}

BOOST_AUTO_TEST_CASE(InsertingIntoNonExistant) {
    for (auto i = test_set_one.begin(); i != test_set_one.end(); ++i)
        BOOST_CHECK_MESSAGE(
            m.insert_into(test_id, i->first, i->second) == false,
            "Inserting into non-existant bloom filter");
}

BOOST_AUTO_TEST_SUITE_END()