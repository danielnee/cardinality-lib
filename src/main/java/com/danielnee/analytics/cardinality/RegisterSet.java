/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.danielnee.analytics.cardinality;

import java.util.Iterator;

/**
 * For 32-bit hashes, we only need 5 bits available in each of our buckets.
 * So using a byte per bucket, wastes 3 bits per bucket. 
 * 
 * The register set uses ints to store the buckets, so we get 6 buckets per int 
 * and waste only 2 bits for each int.
 * 
 * Based mainly on an implementation by Yammer, from
 * 
 * https://github.com/yammer/probablyjs/tree/master/lib/cardinality
 * 
 */
public class RegisterSet implements Iterable<Integer>
{
    private static final int BITS_PER_BUCKET = 5;
    private static final int REGISTERS_PER_BUCKET = 6;
    
    /**
     * The number of bit buckets required by the user
     */
    private int count;
    
    private int[] buckets;
    
    public RegisterSet(int count)
    {
        this.count = count;
        buckets = new int[((int) Math.ceil(count / REGISTERS_PER_BUCKET) + 1)];
    }
    
    public RegisterSet(int count, int[] buckets)
    {
        this.count = count;
        this.buckets = buckets;
    }
    
    public void set(int position, int value)
    {
        int bucketPos = (int) Math.floor(position / REGISTERS_PER_BUCKET);
        int shift = BITS_PER_BUCKET * (position - (bucketPos * REGISTERS_PER_BUCKET));
        this.buckets[bucketPos] = (this.buckets[bucketPos] & ~(0x1f << shift)) | (value << shift);
    }
    
    public int get(int position)
    {
        int bucketPos = (int) Math.floor(position / REGISTERS_PER_BUCKET);
        int shift = BITS_PER_BUCKET * (position - (bucketPos * REGISTERS_PER_BUCKET));
        return (this.buckets[bucketPos] & (0x1f << shift)) >>> shift;
    }
    
    public int getCount()
    {
        return count;
    }
    
    public int getNumberBuckets()
    {
        return buckets.length;
    }
    
    public int[] getBuckets()
    {
        return buckets;
    }

    @Override
    public Iterator<Integer> iterator()
    {
        return new RegisterSetIterator(this);
    }
    
    public class RegisterSetIterator implements Iterator<Integer>
    {
        private RegisterSet set;
        
        private int pos;
        
        public RegisterSetIterator(RegisterSet set)
        {
            this.set = set;
            pos = 0;
        }
        
        @Override
        public boolean hasNext()
        {
            return (pos < set.count);
        }

        @Override
        public Integer next()
        {
            return set.get(pos++);
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }        
    }
}