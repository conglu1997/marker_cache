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

#include "shmbloomfilter.h"
#include <cassert>
#include <cmath>

namespace bf {

shm_bloom_filter::shm_bloom_filter(const void_allocator &void_alloc, size_t m,
                                   size_t k)
    : bits_(m, void_alloc), hasher_(k, void_alloc) {}

bool shm_bloom_filter::lookup(char *data, int data_len) const {
    // the size of a char is 1 byte
    auto digests = hasher_(data, data_len * sizeof(char));
    for (auto d : digests)
        if (!bits_[d % bits_.size()]) return false;
    return true;
}

void shm_bloom_filter::insert(char *data, int data_len) {
    auto digests = hasher_(data, data_len * sizeof(char));
    for (auto d : digests) bits_[d % bits_.size()] = true;
}

// Added for completeness, reset a bloom filter
void shm_bloom_filter::clear() { std::fill(bits_.begin(), bits_.end(), false); }

}  // namespace bf