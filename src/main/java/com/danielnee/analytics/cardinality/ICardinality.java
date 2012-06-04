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

/**
 * Interface all cardinality estimators must implement
 */
public interface ICardinality
{
    /**
     * Provide an estimate of the cardinality
     * @return The current estimate
     */
    public long cardinality();
    
    /**
     * Offer an object to the cardinality estimator.
     * @param o The object to add (will be turned into a string)
     * @return Was the state of the cardinality estimator modified when adding
     * this object
     */
    public boolean offer(Object o);
    
    /**
     * Returns the size of the cardinality estimator in bytes.
     * @return 
     */
    public int sizeof();
}
