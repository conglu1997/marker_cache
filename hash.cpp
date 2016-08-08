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

#include "hash.h"
#include <assert.h>

namespace bf {

hash_function::hash_function(size_t seed) : h3_(seed) {}

size_t hash_function::operator()(char *data, int size) const {
    // Current max is 36 bytes - settable in hash.h
    if (size > max_obj_size) throw std::runtime_error("Object too large!");
    return size == 0 ? 0 : h3_(data, size);
}

hasher::hasher(size_t k, const void_allocator &void_alloc)
    : fns_(void_alloc) {
    assert(k > 0);
	// Seed a LCG with 0 and use this to seed the hash functions
	lcg l(0);
	fns_.reserve(k);
    for (size_t i = 0; i < k; ++i) fns_.push_back(l());
}

std::vector<digest> hasher::operator()(char *data, int size) const {
    std::vector<digest> d(fns_.size());
    for (size_t i = 0; i < fns_.size(); ++i) d[i] = fns_[i](data, size);
    return d;
}

}  // namespace bf