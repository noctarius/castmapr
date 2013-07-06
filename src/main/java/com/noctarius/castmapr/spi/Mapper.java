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

import com.hazelcast.core.IMap;
import com.noctarius.castmapr.MapReduceTask;

/**
 * <p>
 * The abstract Mapper superclass is used to build mappers for the {@link MapReduceTask}. Most mappers will only
 * implement the {@link #map(Object, Object, Collector)} method to collect and emit needed key-value pairs.<br>
 * For more complex algorithms there is the possibility to override the {@link #initialize(Collector)} and
 * {@link #finalized(Collector)} methods as well.
 * </p>
 * <p>
 * A simple mapper could look like the following example:
 * 
 * <pre>
 * public static class MyMapper extends Mapper<Integer, Integer, String, Integer>
 * {
 *   public void map( Integer key, Integer value, Collector<String, Integer> collector )
 *   {
 *     collector.emit( String.valueOf( key ), value );
 *   }
 * }
 * </pre>
 * </p>
 * <p>
 * If you want to know more about the implementation of MapReduce algorithms read the {@see <a
 * href="http://research.google.com/archive/mapreduce-osdi04.pdf">Google Whitepaper on MapReduce</a>}.
 * </p>
 * 
 * @author noctarius
 * @param <KeyIn> The type of key used in the {@link IMap}
 * @param <ValueIn> The type of value used in the {@link IMap}
 * @param <KeyOut> The key type for mapped results
 * @param <ValueOut> The value type for mapped results
 */
public abstract class Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    implements Serializable
{

    /**
     * This method is called before the {@link #map(Object, Object, Collector)} method is executed for every value and
     * can be used to initialize the internal state of the mapper or to emit a special value.
     * 
     * @param collector The {@link Collector} to be used for emitting values.
     */
    public void initialize( Collector<KeyOut, ValueOut> collector )
    {
    }

    /**
     * The map method is called for every single key-value pair in the bound {@link IMap} instance on this cluster node
     * and partition.<br>
     * Due to it's nature of a DataGrid Hazelcast distributes values all over the cluster and so this method is executed
     * on multiple servers at the same time.<br>
     * If you want to know more about the implementation of MapReduce algorithms read the {@see <a
     * href="http://research.google.com/archive/mapreduce-osdi04.pdf">Google Whitepaper on MapReduce</a>}.
     * 
     * @param key
     * @param value
     * @param collector
     */
    public abstract void map( KeyIn key, ValueIn value, Collector<KeyOut, ValueOut> collector );

    /**
     * This method is called after the {@link #map(Object, Object, Collector)} method is executed for every value and
     * can be used to finalize the internal state of the mapper or to emit a special value.
     * 
     * @param collector The {@link Collector} to be used for emitting values.
     */
    public void finalized( Collector<KeyOut, ValueOut> collector )
    {
    }

}
