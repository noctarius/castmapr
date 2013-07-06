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

package com.noctarius.castmapr;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.hazelcast.core.IMap;
import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.MapReduceCollatorListener;
import com.noctarius.castmapr.spi.MapReduceListener;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

/**
 * <p>
 * This interface describes a MapReduceTask that is build by {@link MapReduceTaskFactory#build(IMap)}.<br>
 * It is used to execute mappings and calculations on the different cluster nodes and reduce or collate these mapped
 * values to results.
 * <p>
 * <p>
 * Implementations returned by the MapReduceTaskFactory are fully threadsafe and can be used concurrently and multiple
 * times.
 * </p>
 * <p>
 * <b>Caution: The generic types of MapReduceTasks change depending on the used methods which can make it needed to use
 * different assignment variables when used over multiple source lines.</b>
 * </p>
 * <p>
 * An example on how to use it:
 * 
 * <pre>
 * HazelcastInstance hazelcastInstance = getHazelcastInstance();
 * IMap<Integer, Integer, String, Integer> map = (...) hazelcastInstance.getMap( getMapName() );
 * MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( hazelcastInstance );
 * MapReduceTask<Integer, Integer, String, Integer> task = factory.build( map );
 * Map<String, Integer> results = task.mapper( buildMapper() ).reducer( buildReducer() ).submit();
 * </pre>
 * </p>
 * 
 * @author noctarius
 * @param <KeyIn> The type of key used in the {@link IMap}
 * @param <ValueIn> The type of value used in the {@link IMap}
 * @param <KeyOut> The key type for mapped results
 * @param <ValueOut> The value type for mapped results
 */
public interface MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut>
{

    /**
     * Defines the mapper for this task. This method is not idempotent and is callable only one time. If called further
     * times an {@link IllegalStateException} is thrown telling you to not change the internal state.
     * 
     * @param mapper The tasks mapper
     * @return The instance of this MapReduceTask with generics changed on usage
     */
    MapReduceTask<KeyOut, List<ValueOut>, KeyOut, ValueOut> mapper( Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper );

    /**
     * Defines the reducer for this task. This method is not idempotent and is callable only one time. If called further
     * times an {@link IllegalStateException} is thrown telling you to not change the internal state.
     * 
     * @param reducer The tasks reducer
     * @return The instance of this MapReduceTask with generics changed on usage
     */
    MapReduceTask<KeyOut, ValueOut, KeyOut, ValueOut> reducer( Reducer<KeyOut, ValueOut> reducer );

    /**
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes.
     * 
     * @return The mapped and possibly reduced result.
     */
    Map<KeyIn, ValueIn> submit();

    /**
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes and executes the
     * collator before returning the final result.
     * 
     * @param collator The collator to use after map and reduce
     * @return The mapped, possibly reduced and collated result.
     */
    <R> R submit( Collator<KeyIn, ValueIn, R> collator );

    /**
     * <p>
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes. <br>
     * This method does not block but the given listener is called when the calculation is done and the result is ready.
     * </p>
     * <p>
     * <b>Caution: Compared to {@link #submitAsync(MapReduceListener, ExecutorService)} which executed the background
     * job in the given {@link ExecutorService}, this method will execute the task in the Hazelcast threadpool.</b>
     * </p>
     * 
     * @param listener The {@link MapReduceListener} to call after calculation
     * @return The mapped and possibly reduced result.
     */
    void submitAsync( MapReduceListener<KeyIn, ValueIn> listener );

    /**
     * <p>
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes. <br>
     * This method does not block but the given listener is called when the calculation is done and the result is ready.
     * </p>
     * <p>
     * Compared to {@link #submitAsync(MapReduceListener)} which executed the background job in the Hazelcast
     * threadpool, this method will execute the task in the given {@link ExecutorService}.
     * </p>
     * 
     * @param listener The {@link MapReduceListener} to call after calculation
     * @param executorService The {@link ExecutorService} the background job is executed at
     * @return The mapped and possibly reduced result.
     */
    void submitAsync( MapReduceListener<KeyIn, ValueIn> listener, ExecutorService executorService );

    /**
     * <p>
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes and executes the
     * collator before returning the final result. <br>
     * This method does not block but the given listener is called when the calculation is done and the result is ready.
     * </p>
     * <p>
     * <b>Caution: Compared to {@link #submitAsync(Collator, MapReduceCollatorListener, ExecutorService)} which executed
     * the background job in the given {@link ExecutorService}, this method will execute the task in the Hazelcast
     * threadpool.</b>
     * </p>
     * 
     * @param collator The collator to use after map and reduce
     * @param listener The {@link MapReduceCollatorListener} to call after calculation
     * @return The mapped, possibly reduced and collated result.
     */
    <R> void submitAsync( Collator<KeyIn, ValueIn, R> collator, MapReduceCollatorListener<R> listener );

    /**
     * <p>
     * Submits the task to Hazelcast and executes the defined mapper and reducer on all cluster nodes and executes the
     * collator before returning the final result. <br>
     * This method does not block but the given listener is called when the calculation is done and the result is ready.
     * </p>
     * <p>
     * Compared to {@link #submitAsync(Collator, MapReduceCollatorListener)} which executed the background job in the
     * Hazelcast threadpool, this method will execute the task in the given {@link ExecutorService}.
     * </p>
     * 
     * @param collator The collator to use after map and reduce
     * @param listener The {@link MapReduceCollatorListener} to call after calculation
     * @param executorService The {@link ExecutorService} the background job is executed at
     * @return The mapped, possibly reduced and collated result.
     */
    <R> void submitAsync( Collator<KeyIn, ValueIn, R> collator, MapReduceCollatorListener<R> listener,
                          ExecutorService executorService );

}
