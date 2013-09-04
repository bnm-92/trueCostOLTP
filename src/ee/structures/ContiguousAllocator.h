/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef CONTIGUOUSALLOCATOR_H_
#define CONTIGUOUSALLOCATOR_H_

#include <cstdlib>

namespace voltdb {

/**
 * Dead simple buffer chain that allocates fixed size buffers and
 * fixed size indivual allocations to consumers within those
 * buffers.
 *
 * Note, there are few checks here when running in release mode.
 */
class ContiguousAllocator {
    struct Buffer {
        Buffer *prev;
        char data[0];
    };

    int64_t m_count;
    const int32_t m_allocSize;
    const int32_t m_allocsPerBlock;
    const int32_t m_blockSize;
    int32_t m_blockCount;
    Buffer *m_tail;
    bool m_bigAlloc;

public:
    /**
     * @param allocSize is the size in bytes of individual allocations.
     * @param blockSize is the size of each allocation, naturally bigger than allocSize.
     */
    ContiguousAllocator(int32_t allocSize, int32_t blockSize, bool bigAlloc);
    ~ContiguousAllocator();

    void *alloc();
    void *last() const;
    void trim();
    int64_t count() const { return m_count; }

    size_t bytesAllocated() const;
};

} // namespace voltdb

#endif // CONTIGUOUSALLOCATOR_H_
