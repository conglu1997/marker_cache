rm -f DBAppUnitTests
g++ -o DBAppUnitTests DBAppUnitTests.cpp markercache.cpp shmbloomfilter.cpp mmh3.cpp -I./ -std=c++0x -lrt -pthread
chmod 777 DBAppUnitTests
rm -f SDUnitTests
g++ -o SDUnitTests SDUnitTests.cpp markercache.cpp shmbloomfilter.cpp mmh3.cpp -I./ -std=c++0x -lrt -pthread
chmod 777 SDUnitTests
rm -f TestingSHM
g++ -o TestingSHM TestingSHM.cpp markercache.cpp shmbloomfilter.cpp mmh3.cpp -I./ -std=c++0x -lrt -pthread
chmod 777 TestingSHM
