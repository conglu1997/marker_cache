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

#ifndef BF_HASH_POLICY_H
#define BF_HASH_POLICY_H

#define BOOST_DATE_TIME_NO_LIB
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <functional>
#include "h3.h"

namespace bf {

/// The hash digest type.
typedef size_t digest;

// Allocate our hasher in shared memory as well
typedef boost::interprocess::managed_shared_memory::segment_manager
    segment_manager_t;
typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;

class hash_function {
   public:
    // Max object size in bytes that we will receive
    constexpr static size_t max_obj_size = 36;

    hash_function(size_t seed);

    size_t operator()(char *data, int size) const;

   private:
    h3<size_t, max_obj_size> h3_;
};

typedef boost::interprocess::allocator<hash_function, segment_manager_t>
    hash_fn_allocator;

/// A hasher which hashes a (*, count) pair *k* times.
class hasher {
   public:
    hasher(size_t k, const void_allocator &void_alloc);
    std::vector<digest> operator()(char *data, int size) const;

   private:
    // Each hash function occupies 72KB
    std::vector<hash_function, hash_fn_allocator> fns_;
};

}  // namespace bf

#endif