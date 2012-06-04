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
import java.util.List;

/**
 * Abstract base class for LogLog based cardinality estimation.
 */
abstract public class AbstractLogLog implements ICardinality
{
    protected static final double POWER_2_32 = Math.pow(2.0, 32.0);
    
    protected int m;
    
    protected int k;
    
    protected RegisterSet M;
    
    public AbstractLogLog(int k)
    {
        if (k < 4 || k > Integer.SIZE)
        {
            throw new IllegalArgumentException("Invalid k passed in, k is " + k + ". k must be between 4 and 32");
        }
        
        this.k = k;
        m = (int) Math.pow(2.0, k);
        M = new RegisterSet(m);
    }
    
    public AbstractLogLog(int m, int k, RegisterSet M)
    {
        this.m = m;
        this.k = k;
        this.M = M;
    }
    
    private AbstractLogLog(RegisterSet set)
    {
        M = set;
        m = set.getCount();
        k = (int) Math.round(Math.log(m) / Math.log(2.0));
    }
    
    @Override
    public boolean offer(Object o)
    {
        boolean modified = false;
        int hash = MurmurHash.hash(o.toString().getBytes());
        int j = hash >>> (Integer.SIZE - k);
        byte r = (byte) (Integer.numberOfLeadingZeros((hash << k)) + 1);
        if (r > M.get(j))
        {
            updateStatistics(r, j);
            modified = true;
        }
        
        return modified;
    }
    
    @Override
    public int sizeof()
    {
        return M.getNumberBuckets() * 4;
    }   
         
    
    /**
     * Update the cardinality statistics
     * 
     * @param r - The result of p(y)
     * @param j - The index into M
     */
    abstract protected void updateStatistics(byte r, int j);
    
    /**
     * Applies a correction raw cardinality estimates or extreme cases.
     * 
     * See the papers:
     * - HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
     * - Fast and Accurate Traffic Matrix Measurement Using Adaptive Cardinality Counting
     * 
     * The second paper applies only the small range correction to LogLog estimates. Appears
     * to be no reason not to apply the large range correction as well, so both corrections are
     * applied to LogLog and HyperLogLog estimates.
     * 
     * @param E - The raw estimate before any correction is applied
     * @return - The corrected estimate
     */
    protected long computeCorrectedEstimate(double E)
    {
        if (E <= (5.0 / 2.0) * m)
        {
            // Small range correction
            int i = 0;
            for (int b : M)
            {
                if (b == 0) i++;
            }
            return (long) Math.round(m * Math.log(m / (double) i));
        }
        else if (E <= (1.0 / 30.0) * POWER_2_32)
        {
            // Intermediate range, no correction required
            return (long) Math.round(E);
        }
        else
        {
            // Large range correction
            return (long) Math.round( -POWER_2_32 * Math.log(1 - E / POWER_2_32));
        }
    }
    
    protected static AbstractLogLog mergeRegisterSets(List<? extends AbstractLogLog> counters) throws CardinalityMergeException
    {
        if (counters != null && counters.size() > 0)
        {
            int countersSize = counters.size();
            AbstractLogLog baseCounter = counters.get(0);
            RegisterSet set = baseCounter.M;
            int size = baseCounter.m;
            
            for (int i = 1; i < countersSize; ++i)
            {
                if (counters.get(i).m != size)
                {
                    throw new CardinalityMergeException("Cannot merge counters of different sizes");
                }
                
                RegisterSet otherSet = counters.get(i).M;
                for (int j = 0; j < size; ++j)
                {
                    int otherByte = otherSet.get(j);
                    if (otherByte > set.get(j))
                    {
                        baseCounter.updateStatistics((byte) otherByte, j);
                    }
                }
            }
            return baseCounter; 
        }
        return null;
    }
}
