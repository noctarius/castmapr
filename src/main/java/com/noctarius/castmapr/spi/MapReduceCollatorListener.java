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

import com.noctarius.castmapr.MapReduceTask;

/**
 * This listener is used for retrieving asynchronous results on {@link MapReduceTask} using {@link Mapper},
 * {@link Reducer} <b>AND</b> {@link Collator}.<br>
 * <b>Caution: Implementations need to be fully threadsafe!</b>
 * 
 * @author noctarius
 * @param <Key> The type of keys
 * @param <Value> The type of values
 */
public interface MapReduceCollatorListener<R>
{

    /**
     * This method is called when a calculation of the {@link MapReduceTask} is finished.
     * 
     * @param reducedResults The mapped, reduced and collated result.
     */
    void onCompletion( R result );

}
