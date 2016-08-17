rm UnitTests
g++ -o UnitTests UnitTests.cpp markercache.cpp shmbloomfilter.cpp hash.cpp -I/. -std=c++0x -lrt -pthread
chmod 777 UnitTests
./UnitTests
