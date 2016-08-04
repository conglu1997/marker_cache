#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "markercache.h"

using namespace std;

// Generate length-8 random strings for testing
// Need to delete after using test data.
vector<char*> generate_test_data(size_t num_elems, size_t width) {
    vector<char*> v;
    string chars(
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "1234567890"
        "!@#$%^&*()"
        "`~-_=+[{]}\\|;:'\",<.>/? ");
    default_random_engine rng(random_device{}());
    uniform_int_distribution<size_t> distribution(0, chars.size() - 1);
    for (auto i = 0; i < num_elems; ++i) {
        auto s = new char[width];
        for (size_t i = 0; i < width; ++i) s[i] = chars[distribution(rng)];
        v.push_back(s);
    }

    return v;
}

int main() {
    // Clear shared memory object if it exists before creation, this will not be
    // done on the search director side
    boost::interprocess::shared_memory_object::remove("BFSharedMemory");
    auto bytes_allocated = 100000000;
    marker_cache m(bytes_allocated);  // bytes -- on the order of 100MB

    auto test_size = 1000000;  // 1 million
    auto test_fprate = 0.001;  // 1 in 1 thousand
    auto test_width = 8;       // 8 byte char[]
    auto falsepos = 0;
    static_assert(sizeof(char) == 1, "Chars used are the correct size.");

    cout << "Object occupies " << m.create(1, test_fprate, test_size, 0)
         << " bytes." << endl;

    cout << "Priming test data:" << endl;

    auto v = generate_test_data(test_size, test_width);
    auto v2 = generate_test_data(test_size, test_width);

    cout << "Test data generated." << endl;

    auto begin = chrono::steady_clock::now();

    for (auto str : v) m.insert_into(1, str, test_width);

    auto end = chrono::steady_clock::now();

    cout << "Finished " << test_size << " inserts in "
         << chrono::duration_cast<chrono::milliseconds>(end - begin).count()
         << " milliseconds." << endl;

    begin = chrono::steady_clock::now();

    for (auto str : v) {
        if (m.lookup_from(1, str, test_width) == 0) {
            cout << "WRONG - ABORT ABORT" << endl;
            // Something horribly wrong has gone on here, bloom filter error
        }
    }

    end = chrono::steady_clock::now();

    cout << "Finished " << test_size << " checks in "
         << chrono::duration_cast<chrono::milliseconds>(end - begin).count()
         << " milliseconds." << endl;

    for (auto str : v) delete[] str;

    begin = chrono::steady_clock::now();

    for (auto str : v2) {
        if (m.lookup_from(1, str, 8)) {
            falsepos++;
        }
    }

    end = chrono::steady_clock::now();

    cout << "Finished " << test_size << " false positive checks in "
         << chrono::duration_cast<chrono::milliseconds>(end - begin).count()
         << " milliseconds. Observed fp rate: "
         << (double)falsepos / (double)test_size
         << ", Desired fp rate: " << test_fprate << endl;

    for (auto str : v2) delete[] str;

    cin.get();
    return 0;
}