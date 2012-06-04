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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;

/**
 * Simple hash table implementation of cardinality counting. Gives the
 * exact result that all approximate methods can be compared to.
 */
public class HashCounter implements ICardinality
{
    private HashSet<String> set;
    
    public HashCounter()
    {
        set = new HashSet<String>();
    }
    
    public HashCounter(int initialSize)
    {
        set = new HashSet<String>(initialSize);
    }
    
    private HashCounter(HashSet<String> set)
    {
        this.set = set;
    }

    @Override
    public long cardinality()
    {
        return set.size();
    }

    @Override
    public boolean offer(Object o)
    {
        return set.add(o.toString());
    }

    @Override
    public int sizeof()
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this);
            oos.close();
        }
        catch (IOException e)
        {
        }
        return baos.size();
    }
    
    public static HashCounter mergeEstimators(HashCounter... counters)
    {
        if (counters != null && counters.length > 0)
        {
            HashSet<String> set = new HashSet<String>();
            for (int i = 0; i < counters.length; ++i)
            {
                set.addAll(counters[i].set);
            }
            return new HashCounter(set);
        }
        return null;
    }
}
