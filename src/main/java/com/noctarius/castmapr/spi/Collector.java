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

/**
 * The Collector interface is used for emitting keys and values to the sample space of the MapReduce algorith.
 * 
 * @author noctarius
 * @param <Key> The key type of the resulting keys
 * @param <Value> The value type of the resulting values
 */
public interface Collector<Key, Value>
{

    /**
     * Emits a key-value pair to the sample space. The same key can be used multiple times to collect values under the
     * same key.
     * 
     * @param key The emitted key.
     * @param value The emitted value.
     */
    void emit( Key key, Value value );

}
