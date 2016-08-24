#include <markercache.h>
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

vector<pair<char *, int>> generate_test_data(size_t num_elems, size_t min_width,
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

int main() {
    auto test_fprate = 0.001;
    size_t test_size = 100000;

    size_t dur = 30;
    size_t lifespan = 90;
    size_t num_filters = std::ceil(lifespan / dur) + 1;

    size_t min_test_width = 50;
    size_t max_test_width = 250;

    marker_cache m(dur, lifespan, test_fprate, test_size * num_filters);

    // Fill up the cache with data
    for (size_t i = 0; i < 3; ++i) {
        // Don't save this data
        m.maybe_age(true, false);
        vector<pair<char *, int>> test_set =
            generate_test_data(test_size, min_test_width, max_test_width);

        for (vector<pair<char *, int>>::const_iterator i = test_set.cbegin();
             i != test_set.cend(); ++i)
            m.insert(i->first, i->second);

        for (vector<pair<char *, int>>::iterator i = test_set.begin();
             i != test_set.end(); ++i)
            delete i->first;
    }

    cout << "Finished preparing shared memory" << endl;
    cin.get();
}