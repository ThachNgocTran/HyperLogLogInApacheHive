/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.stream.cardinality;

public class RegisterSet {

    /// Because Register (Bucket) size is 5. In an Int (32 bit), one can contain 6 buckets. (2 bits are wasteful anyways!)
    public final static int LOG2_BITS_PER_WORD = 6;

    /// hardcoded with 5!
    public final static int REGISTER_SIZE = 5;

    /// count = the number of buckets ==> power(2, p)
    public final int count;
    public final int size;

    private final int[] M;

    public RegisterSet(int count) {
        this(count, null);
    }

    public RegisterSet(int count, int[] initialValues) {
        this.count = count;

        if (initialValues == null) {
            this.M = new int[getSizeForCount(count)];
        } else {
            this.M = initialValues;
        }
        this.size = this.M.length;
    }

    public static int getBits(int count) {
        return count / LOG2_BITS_PER_WORD;
    }

    public static int getSizeForCount(int count) {
        int bits = getBits(count);
        if (bits == 0) {
            return 1;
        } else if (bits % Integer.SIZE == 0) {
            return bits;
        } else {
            return bits + 1;
        }
    }

    public void set(int position, int value) {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        this.M[bucketPos] = (this.M[bucketPos] & ~(0x1f << shift)) | (value << shift);
    }

    public int get(int position) {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (this.M[bucketPos] & (0x1f << shift)) >>> shift;
    }

    /// Only update if greater!!!
    public boolean updateIfGreater(int position, int value) {
        /// find the bucket in the integer array. Remember: one integer can contain 6 buckets. (6x5=30 bits)
        int bucket = position / LOG2_BITS_PER_WORD;

        /// For example: position = 5 ==> bucket = 0 ==> (position - (bucket * LOG2_BITS_PER_WORD)) = 5
        /// ==> shift = 25
        int shift = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
        int mask = 0x1f << shift; /// ==> 0011 1110 0000 0000 0000 0000 0000 0000 ==> 32 bits
        /// 1F = 11111 ==> also the size of register

        if (value >= 32 && shift < 25){
            // Could not test this case even counting 2 billions distinct items
            // This case needs testing because when overflow with shift < 25, the nearest bucket on the left will be overwritten!
            value = value;
        }

        // Use long to avoid sign issues with the left-most shift
        long curVal = this.M[bucket] & mask;   /// get the current value in the indicated bucket

        /// Instead of shifting "curVal" back to real number  (0 <= curVal <= 31), we shift the target value to the same!
        long newVal = value << shift;
        if (curVal < newVal) {
            /// (this.M[bucket] & ~mask) is to reset the bucket at the indicated position
            this.M[bucket] = (int) ((this.M[bucket] & ~mask) | newVal);
            return true;
        } else {
            return false;
        }

        /* In one case: 100mil with Uniform Distribution
        Mask :0011 1110 0000 0000 0000 0000 0000 0000
        CurrV:0001 0110 0000 0000 0000 0000 0000 0000
        NewVa:0100 0000 0000 0000 0000 0000 0000 0000
        FullM:0001 0110 1001 0101 1010 1001 0010 1100
        ~Mask:1100 0001 1111 1111 1111 1111 1111 1111
        After:0100 0000 1001 0101 1010 1001 0010 1100
         */

        /*
        According to http://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll/
        Some registers being overflowed are not a big issue, especially if one has thousands of registers like HLL 16 bits.
        Nevertheless, the ideal max cardinality is 2^31 = 2 billions.
         */
    }

    public void merge(RegisterSet that) {
        for (int bucket = 0; bucket < M.length; bucket++) {
            int word = 0;
            for (int j = 0; j < LOG2_BITS_PER_WORD; j++) {
                int mask = 0x1f << (REGISTER_SIZE * j);

                int thisVal = (this.M[bucket] & mask);
                int thatVal = (that.M[bucket] & mask);
                word |= (thisVal < thatVal) ? thatVal : thisVal;
            }
            this.M[bucket] = word;
        }
    }

    int[] readOnlyBits() {
        return M;
    }

    public int[] bits() {
        int[] copy = new int[size];
        System.arraycopy(M, 0, copy, 0, M.length);
        return copy;
    }
}
