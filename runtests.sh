rm DBAppUnitTests
g++ -o DBAppUnitTests DBAppUnitTests.cpp markercache.cpp shmbloomfilter.cpp mmh3.cpp -I/export/home/build/V9.4_product_LV4_Patch_CL5_1/sas/code/ced/dbapp -std=c++0x -lrt -pthread
chmod 777 DBAppUnitTests
rm SDUnitTests
g++ -o SDUnitTests SDUnitTests.cpp markercache.cpp shmbloomfilter.cpp mmh3.cpp -I/export/home/build/V9.4_product_LV4_Patch_CL5_1/sas/code/ced/dbapp -std=c++0x -lrt -pthread
chmod 777 SDUnitTests
