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

import com.hazelcast.core.MultiMap;

/**
 * This interface can be used to mark {@link Mapper} or {@link Reducer} implementation being aware of the
 * {@link MultiMap} they are being executed aginst.
 * 
 * @author noctarius
 * @param <Key> The key type
 * @param <Value> The value
 */
public interface MultiMapAware<Key, Value>
{

    /**
     * This method is called after deserializing but before executing {@link Mapper#map(Object, Object, Collector)} or
     * {@link Reducer#reduce(Object, java.util.Iterator)}.
     * 
     * @param multiMap The multiMap the implementing instance is executed against.
     */
    void setMultiMap( MultiMap<Key, Value> multiMap );

}
