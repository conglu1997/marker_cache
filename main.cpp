#include <boost/chrono.hpp>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "markercache.h"

using namespace std;

// Generate length-8 random strings for testing
// Need to delete after using test data.
vector<pair<char*, int>> generate_test_data(size_t num_elems, size_t min_width,
                                            size_t max_width, size_t seed) {
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
    int width = wdistribution(rng);

    for (size_t i = 0; i < num_elems; ++i) {
        char* s = new char[width];
        for (size_t i = 0; i < width; ++i) s[i] = chars[distribution(rng)];
        v.push_back(pair<char*, int>(s, width));
    }

    return v;
}

int main() {
    // Clear shared memory object if it exists before creation, this will not be
    // done on the search director side
    boost::interprocess::shared_memory_object::remove("BFSharedMemory");
    size_t bytes_allocated = 100000000;
    marker_cache m(bytes_allocated);  // bytes -- on the order of 100MB

    size_t test_size = 1000000;  // 1 million
    double test_fprate = 0.01;   // 1 in 1
    size_t min_test_width = 8;
    size_t max_test_width = 16;
    size_t falsepos = 0;
    assert(sizeof(char) == 1);  // Char used is correct size

    // Create a bloom filter with id 1
    cout << "Object occupies " << m.create(1, test_fprate, test_size)
         << " bytes." << endl;

    cout << "Priming test data:" << endl;

    vector<pair<char*, int>> v =
        generate_test_data(test_size, min_test_width, max_test_width, 36);
    vector<pair<char*, int>> v2 =
        generate_test_data(test_size, min_test_width, max_test_width, 48);

    cout << "Test data generated." << endl;

    boost::chrono::time_point<boost::chrono::steady_clock> begin =
        boost::chrono::steady_clock::now();

    for (vector<pair<char*, int>>::const_iterator i = v.begin(); i != v.end();
         ++i)
        m.insert_into(1, i->first, i->second);

    boost::chrono::time_point<boost::chrono::steady_clock> end =
        boost::chrono::steady_clock::now();

    cout << "Finished " << test_size << " inserts in "
         << boost::chrono::duration_cast<boost::chrono::milliseconds>(end -
                                                                      begin)
                .count()
         << " milliseconds." << endl;

    begin = boost::chrono::steady_clock::now();

    for (vector<pair<char*, int>>::const_iterator i = v.cbegin(); i != v.cend();
         ++i) {
        if (m.lookup_from(1, i->first, i->second) == 0) {
            cout << "WRONG - ABORT ABORT" << endl;
            // Something horribly wrong has gone on here, bloom filter error
        }
    }

    end = boost::chrono::steady_clock::now();

    cout << "Finished " << test_size << " checks in "
         << boost::chrono::duration_cast<boost::chrono::milliseconds>(end -
                                                                      begin)
                .count()
         << " milliseconds." << endl;

    for (vector<pair<char*, int>>::iterator i = v.begin(); i != v.end(); ++i)
        delete i->first;

    begin = boost::chrono::steady_clock::now();

    for (vector<pair<char*, int>>::const_iterator i = v2.cbegin();
         i != v2.cend(); ++i) {
        if (m.lookup_from(1, i->first, i->second)) {
            ++falsepos;
        }
    }

    end = boost::chrono::steady_clock::now();

    cout << "Finished " << test_size << " false positive checks in "
         << boost::chrono::duration_cast<boost::chrono::milliseconds>(end -
                                                                      begin)
                .count()
         << " milliseconds. Observed fp rate: "
         << (double)falsepos / (double)test_size
         << ", Desired fp rate: " << test_fprate << endl;

    for (vector<pair<char*, int>>::iterator i = v2.begin(); i != v2.end(); ++i)
        delete i->first;

    cin.get();
    return 0;
}