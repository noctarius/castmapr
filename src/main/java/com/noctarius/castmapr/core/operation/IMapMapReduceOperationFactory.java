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

package com.noctarius.castmapr.core.operation;

import com.hazelcast.spi.Operation;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

public class IMapMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>
{

    public IMapMapReduceOperationFactory()
    {
        super();
    }

    public IMapMapReduceOperationFactory( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                          Reducer<KeyOut, ValueOut> reducer, boolean distributableReducer )
    {
        super( name, mapper, reducer, distributableReducer );
    }

    @Override
    public Operation createOperation()
    {
        Reducer r = distributableReducer ? reducer : null;
        return new IMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, r, null );
    }

}
