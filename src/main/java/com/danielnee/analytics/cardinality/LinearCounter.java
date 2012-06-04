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

import com.danielnee.analytics.hash.MurmurHash;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
* See <i>A Linear-Time Probabilistic Counting Algorithm for Database Applications</i>
* by Whang, Vander-Zanden, Taylor
*
*/
public class LinearCounter implements ICardinality
{
    /**
     * Bitset.
     */
    private byte[] set;
    
    /**
     * The size of the bitset in bits.
     */
    private int size;
    
    /**
     * Number of unset bits in the bitset.
     */
    private int count;
    
    /**
     * Constructor
     * @param size - The number of bytes for the initial bitset.
     */
    public LinearCounter(int size)
    {
        this.size = 8 * size;
        this.count = this.size;
        this.set = new byte[size];
    }
    
    private LinearCounter(byte[] set)
    {
        size = set.length;
        this.set = set;
        count = computeCount();
    }

    @Override
    public long cardinality()
    {
        return Math.round((double) size * Math.log(((double) size / (double) count)));
    }

    @Override
    public boolean offer(Object o)
    {
        boolean modified = false;
        
        long hash = (long) MurmurHash.hash(o.toString().getBytes());
        int bit = (int) ((hash & 0xFFFFFFFFL) % (long) size); // Ensure two-complement no wierdness? Check
        // Find the relevant byte
        int i = bit / 8;
        byte b = set[i];
        byte mask = (byte) (1 << (bit % 8));
        if ((mask & b) == 0) // Do we need to modify the byte?
        {
            set[i] = (byte) (b | mask);
            count--;
            modified = true;
        }
        
        return modified;
    }
    
    private int computeCount()
    {
        int c = 0;
        for (byte b : set)
        {
            c += Integer.bitCount(b & 0xFF);
        }
        return size - c;
    }

    @Override
    public int sizeof()
    {
        return set.length;
    }
    
    public static LinearCounter mergeEstimators(LinearCounter... counters) throws CardinalityMergeException
    {
        if (counters != null && counters.length > 0)
        {
            int size = counters[0].size;
            byte[] data = counters[0].set;
            
            for (int i = 1; i < counters.length; ++i)
            {
                if (size != counters[i].size)
                {
                    throw new CardinalityMergeException("Cannot merge counters of different sizes");
                }
                for (int j = 0; j < counters[i].size; ++j)
                {
                    data[j] |= counters[i].set[j];
                }
            }
            return new LinearCounter(data);
        }
        return null;
    }
    
    public static class Builder implements IBuilder<ICardinality>
    {
        protected static final int MIN_CARDINALITY = 100;
        protected static final int MAX_CARDINALITY = 120000000;
        protected static final int ABOVE_MAX_FACTOR = 11;
        
        protected static final TreeMap<Integer, Integer> onePercentError;
        static
        {
            onePercentError = new TreeMap<Integer, Integer>();
            onePercentError.put(100,5034);
            onePercentError.put(200,5067);
            onePercentError.put(300,5100);
            onePercentError.put(400,5133);
            onePercentError.put(500,5166);
            onePercentError.put(600,5199);
            onePercentError.put(700,5231);
            onePercentError.put(800,5264);
            onePercentError.put(900,5296);
            onePercentError.put(1000,5329);
            onePercentError.put(2000,5647);
            onePercentError.put(3000,5957);
            onePercentError.put(4000,6260);
            onePercentError.put(5000,6556);
            onePercentError.put(6000,6847);
            onePercentError.put(7000,7132);
            onePercentError.put(8000,7412);
            onePercentError.put(9000,7688);
            onePercentError.put(10000,7960);
            onePercentError.put(20000,10506);
            onePercentError.put(30000,12839);
            onePercentError.put(40000,15036);
            onePercentError.put(50000,17134);
            onePercentError.put(60000,19156);
            onePercentError.put(70000,21117);
            onePercentError.put(80000,23029);
            onePercentError.put(90000,24897);
            onePercentError.put(100000,26729);
            onePercentError.put(200000,43710);
            onePercentError.put(300000,59264);
            onePercentError.put(400000,73999);
            onePercentError.put(500000,88175);
            onePercentError.put(600000,101932);
            onePercentError.put(700000,115359);
            onePercentError.put(800000,128514);
            onePercentError.put(900000,141441);
            onePercentError.put(1000000,154171);
            onePercentError.put(2000000,274328);
            onePercentError.put(3000000,386798);
            onePercentError.put(4000000,494794);
            onePercentError.put(5000000,599692);
            onePercentError.put(6000000,702246);
            onePercentError.put(7000000,802931);
            onePercentError.put(8000000,902069);
            onePercentError.put(9000000,999894);
            onePercentError.put(10000000,1096582);
            onePercentError.put(50000000,4584297);
            onePercentError.put(100000000,8571013);
            onePercentError.put(120000000,10112529);
            // TODO: Use Collections.unmodifiableMap()
        }
       
        protected int size;
        
        public Builder(int size)
        {
            this.size = size;
        }
        
        @Override
        public LinearCounter build()
        {
            return new LinearCounter(size);
        }
        
        public static Builder onePercentError(int maxCardinality)
        {
            if (maxCardinality <= 0)
            {
                throw new IllegalArgumentException("Max cardinaility must be a positive integer, given " + maxCardinality);
            }
            
            int length = 0;
            if (maxCardinality <= MIN_CARDINALITY)
            {
                length = onePercentError.firstEntry().getValue();
            }
            else if (maxCardinality >= MAX_CARDINALITY)
            {
                length = maxCardinality / ABOVE_MAX_FACTOR;
            }
            else
            {
                // Find the two interval points
                Entry<Integer, Integer> fromValue = onePercentError.floorEntry(maxCardinality);
                Entry<Integer, Integer> toValue = onePercentError.higherEntry(fromValue.getKey());
                // TODO: Probably a faster storage/retrieval mechanism for finding the interval than treemap but neglible
                // performance improvement (i.e. O(log(n) + 1 vs. current 2log(n) )
                length = linearInterpolation(maxCardinality, fromValue.getKey(), fromValue.getValue(),
                        toValue.getKey(), toValue.getValue());
            }
            
            int byteLength = (int) Math.ceil(length / 8D);
            return new Builder(byteLength);
        }
        
        /**
         * Standard linear interpolation to find the y value
         * See http://en.wikipedia.org/wiki/Linear_interpolation
         * 
         * @param x
         * @param x0
         * @param y0
         * @param x1
         * @param y1
         * @return 
         */
        protected static int linearInterpolation(int x, int x0, int y0, int x1, int y1)
        {
            return (int) Math.ceil(y0 + ((x - x0) * y1  - (x - x0) * y0) / (double) (x1 - x0));
        }
    }
}
