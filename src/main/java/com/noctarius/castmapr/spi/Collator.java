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

import java.util.Map;

/**
 * This interface can be implemented to define a Collator which is executed after calculation of the MapReduce algorithm
 * on remote cluster nodes but before returning the final result.<br>
 * Collator can for example be used to sum up a final value.
 * 
 * @author noctarius
 * @param <Key> The key type of the resulting keys
 * @param <Value> The value type of the resulting values
 * @param <R> The type for the collated result
 */
public interface Collator<Key, Value, R>
{

    /**
     * This method is called with the mapped and possibly reduced values from the MapReduce algorithm.
     * 
     * @param reducedResults The mapped and possibly reduced intermediate results.
     * @return The collated result.
     */
    R collate( Map<Key, Value> reducedResults );

}
