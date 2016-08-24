# marker_cache

char* bloom filter implemented in shared memory

To run SDUnitTests, compile TestingSHM.cpp and run it in the background and then run SDUnitTests.

The libaries:
  - boost::filesystem
  - boost::serialization

require separate compilation. The process is described here: http://www.boost.org/doc/libs/1_61_0/more/getting_started/unix-variants.html#prepare-to-use-a-boost-library-binary
