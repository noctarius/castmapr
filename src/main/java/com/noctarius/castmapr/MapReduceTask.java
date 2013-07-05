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

import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

public interface MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut>
{

    MapReduceTask<KeyOut, List<ValueOut>, KeyOut, ValueOut> mapper( Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper );

    MapReduceTask<KeyOut, ValueOut, KeyOut, ValueOut> reducer( Reducer<KeyOut, ValueOut> reducer );

    Map<KeyIn, ValueIn> submit();

    <R> R submit( Collator<KeyIn, ValueIn, R> collator );

    void submitAsync( MapReduceListener<KeyIn, ValueIn> listener );

    <R> void submitAsync( Collator<KeyIn, ValueIn, R> collator, MapReduceCollatorListener<R> listener );

}
