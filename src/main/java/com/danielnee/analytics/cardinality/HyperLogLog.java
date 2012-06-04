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

import java.util.List;

/**
 * Implementation of the HyperLogLog algorithm for cardinality estimation.
 * 
 * Generally offers the best performance in terms of memory usage and 
 * accuracy compared to LogLog and LinearCounter.
 * 
 * Implementation is based on the paper
 * 
 * HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
 */
public class HyperLogLog extends AbstractLogLog
{
    protected double alphaMM;
    
    /**
     * Create a HyperLogLog object with the required value of k.
     * @param k 
     */
    public HyperLogLog(int k)
    {
        super(k);
        InitialiseAlphaMM();
    }
    
    /**
     * Create a HyperLogLog object based on a required standard error. For instance,
     * if you want a 1% standard error, pass in 0.01 and the appropriate value of k will be found.
     * 
     * In practical scenarios, the error will not be exactly the rse you pass in (as this is a 
     * theoretical standard error), but in general practical results match the theoretical ones pretty well.
     * 
     * @param rse The required standard error 
     */
    public HyperLogLog(double rse)
    {
        this(ComputeRequiredK(rse));
    }
   
    public HyperLogLog(int m, int k, RegisterSet M, double alphaMM)
    {
        super(m, k, M);
        this.alphaMM = alphaMM;
    }
    
    /**
     * Computed the required k to achieve the theoretical standard error
     * @param rse The standard error you wish to achieve.
     * @return The value of k 
     */
    private static int ComputeRequiredK(double rse)
    {
        return (int) Math.ceil((Math.log(1.0816)- 2.0 * Math.log(rse)) / Math.log(2.0));
    }

    @Override
    public long cardinality()
    {
        // Compute the indicator function
        double Z = 0.0;
        for (int i : M)
        {
            Z += Math.pow(2.0, -i);
        }
        
        // Compute the raw estimate
        double E = alphaMM * (1.0 / Z);
        
        // Perform the corrections as listed in the paper
        return computeCorrectedEstimate(E);
    }

    @Override
    protected void updateStatistics(byte r, int j)
    {
        M.set(j, r);
    }
    
    private void InitialiseAlphaMM()
    {
        switch (this.m)
        {
            case 4:
                alphaMM = 0.673 * m * m;
                break;
            case 5:
                alphaMM =  0.697 * m * m;
                break;
            case 6:
                alphaMM = 0.709 * m * m;
                break;
            default:
                alphaMM = m * m * (0.7213 / (1 + 1.079 / m));
                break;
        }
    }
    
    public static HyperLogLog mergeEstimators(List<HyperLogLog> counters) throws CardinalityMergeException
    {
        // Downcast the return result to a HyperLogLog object.
        return (HyperLogLog) mergeRegisterSets(counters);
    }
}
