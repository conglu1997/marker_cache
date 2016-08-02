#include <iostream>
#include "marker_cache.h"

using namespace std;

int main() {
    // Clear shared memory object before creation
    boost::interprocess::shared_memory_object::remove("BFSharedMemory");
    marker_cache m(1000000);

    m.create(1, 0.01, 1000, 0, false, false);
    char a[] = "string";

	// we can use sizeof directly since char is 1 byte
    cout << m.lookup_from(1, a, sizeof(a)) << endl;
	
    m.insert_into(1, a, sizeof(a) / sizeof(a[0]));
    cout << m.lookup_from(1, a, sizeof(a)) << endl;

    cin.get();
    return 0;
}