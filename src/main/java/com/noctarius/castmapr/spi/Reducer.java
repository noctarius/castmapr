/*
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

package com.noctarius.castmapr.spi;

import java.io.Serializable;
import java.util.Iterator;

import com.noctarius.castmapr.MapReduceTask;

/**
 * <p>
 * The Reducer interface is used to build reducers for the {@link MapReduceTask}. Most reducers are relatively simple
 * and capable of running in a distributed manner.<br>
 * To mark a {@link Reducer} implementation as ditributable you need to explicitly mark is using the
 * {@link Distributable} type-level annotation or by implementing the {@link DistributableReducer} interface.
 * </p>
 * <p>
 * A simple Reducer implementation could look like that sum-function implementation
 * 
 * <pre>
 * public static class TestReducer implements Reducer<String, Integer>
 * {
 *   public Integer reduce( String key, Iterator<Integer> values )
 *   {
 *     int sum = 0;
 *     while ( values.hasNext() )
 *     {
 *       sum += values.next();
 *     }
 *     return sum;
 *   }
 * }
 * </pre>
 * </p>
 * <p>
 * <b>Caution: For distributable {@link Reducer}s you need to pay attention that the reducer is executed multiple times
 * and only with intermediate results of one cluster node! This is totally ok for example for sum-algorithms but can be
 * a problem for other kinds!</b>
 * </p>
 * 
 * @author noctarius
 * @param <Key> The key type of the resulting keys
 * @param <Value> The value type of the resulting values
 */
public interface Reducer<Key, Value>
    extends Serializable
{

    /**
     * The reduce method is called either locally after of intermediate results are retrieved from mapping algorithms or
     * (if the Reducer implementation is distributable - see {@link Distributable} or {@link DistributableReducer}) on
     * the different cluster nodes.<br>
     * <b>Caution: For distributable {@link Reducer}s you need to pay attention that the reducer is executed multiple
     * times and only with intermediate results of one cluster node! This is totally ok for example for sum-algorithms
     * but can be a problem for other kinds!</b><br>
     * {@link Reducer} implementations are never distributed by default - they need to explicitly be marked using the
     * {@link Distributable} annotation or by implementing the {@link DistributableReducer} interface.
     * 
     * @param key The reduced key
     * @param values The values corresponding to the reduced key
     * @return The reduced value
     */
    Value reduce( Key key, Iterator<Value> values );

}
