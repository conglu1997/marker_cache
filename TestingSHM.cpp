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
    marker_cache m(1000000);
    vector<pair<char *, int>> test_set;
    const static size_t test_size = 100000;
    const static size_t min_test_width = 8;
    const static size_t max_test_width = 16;

    for (vector<pair<char *, int>>::const_iterator i = test_set.cbegin();
         i != test_set.cend(); ++i)
        m.insert_into(1, i->first, i->second);

    cout << "Finished preparing shared memory" << endl;

    cin.get();

    for (vector<pair<char *, int>>::iterator i = test_set.begin();
         i != test_set.end(); ++i)
        delete i->first;
}