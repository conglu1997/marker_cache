rm UnitTests
g++ -o UnitTests UnitTests.cpp markercache.cpp shmbloomfilter.cpp hash.cpp -I/export/home/build/V9.4_product_LV4_Patch_CL5_1/sas/code/ced/dbapp -std=c++0x -lrt -pthread
chmod 777 UnitTests
./UnitTests
