/**
Copyright (c) 2016, Matthias Vallentin
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
**/

#ifndef BF_BLOOM_FILTER_SHM_H
#define BF_BLOOM_FILTER_SHM_H

#include <hash.h>
#include <boost/dynamic_bitset.hpp>
#include <boost/interprocess/containers/map.hpp>

namespace bf {

typedef boost::interprocess::allocator<bool, segment_manager_t> bool_allocator;
typedef size_t block_t;
typedef boost::interprocess::allocator<block_t, segment_manager_t>
    block_allocator;
typedef boost::dynamic_bitset<block_t, block_allocator> bitset;

// Use the vector<bool> 1-bit per bool optimisation
// typedef std::vector<bool, bool_allocator> bitset;

class shm_bloom_filter {
   public:
    shm_bloom_filter(const void_allocator &void_alloc, size_t m, size_t k);

    bool lookup(char *data, int data_len) const;
    void insert(char *data, int data_len);

   private:
    hasher hasher_;
    bitset bits_;
};

}  // namespace bf

#endif
