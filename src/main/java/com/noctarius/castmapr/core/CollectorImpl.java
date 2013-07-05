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

package com.noctarius.castmapr.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.noctarius.castmapr.spi.Collector;

public class CollectorImpl<Key, Value>
    implements Collector<Key, Value>
{

    public final Map<Key, List<Value>> emitted = new HashMap<Key, List<Value>>();

    @Override
    public void emit( Key key, Value value )
    {
        List<Value> values = emitted.get( key );
        if ( values == null )
        {
            values = new LinkedList<Value>();
            emitted.put( key, values );
        }
        values.add( value );
    }
}
