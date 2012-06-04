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
 * Implementation of LogLog estimation of cardinalities.
 * 
 * Has some small improvements over original paper mainly dealing with cases when
 * m or n are very large (estimation correction).
 * 
 * See <i>Loglog Counting of Large Cardinalities</i>
 * by M. Durand and P. Flajolet
 */
public class LogLog extends AbstractLogLog
{
    /**
     * Pre-computed values of m * alpha.
     * 
     * Computed using R:
     * 
     * a = seq(0,6);
     * m = 2^a;
     * alpha = m * (gamma(-m^(-1)) * ((1-2^m^(-1)) / log(2)))^-m
     * 
     * In R, at least, this computation becomes unstable after m > 20
     */
    private static final double[] alpha =
    {
        0, 0.222839630027075, 0.312015983556785, 0.354890690500987, 
        0.376032697405068, 0.386541248923512, 0.391781118798575
    };
    
    private static final double alphaInfinity = 0.39701;
    
    private int MSum;
    
    private double mAlpha;
    
    public LogLog(int k)
    {
        super(k);
        if (k <= 0 || k >= Integer.SIZE)
        {
            throw new IllegalArgumentException("Invalid k passed to LogLog. k was" + k);
        }
        
        if (k <= 6)
        {
            mAlpha = m * alpha[k];
        }
        else
        {
            // For m >= 64, the alpha m can be replaced with alpha ininity without much
            // detectable bias, see paper
            mAlpha = m * alphaInfinity;
        }
    }
    
    public LogLog(double rse)
    {
        this(ComputeRequiredK(rse));
    }
    
    private static int ComputeRequiredK(double rse)
    {
        return (int) Math.ceil((Math.log(1.69)- 2.0 * Math.log(rse)) / Math.log(2.0));
    }   
       
    @Override
    public long cardinality()
    {
        double MAverage = MSum / (double) m;
        double E = Math.round(mAlpha * Math.pow(2.0, MAverage));
        return computeCorrectedEstimate(E);
    }
    
    @Override
    protected void updateStatistics(byte r, int j)
    {
        MSum += r - M.get(j); // Only increase by the delta
        M.set(j, r);
    }
    
    public static LogLog mergeEstimators(List<LogLog> counters) throws CardinalityMergeException
    {
        // Downcast the return result to a LogLog object.
        return (LogLog) mergeRegisterSets(counters);
    }
}
