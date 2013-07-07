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

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

public class MultiMapMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>
    implements OperationFactory
{

    private Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    private Reducer<KeyOut, ValueOut> reducer;

    private boolean distributableReducer;

    private String name;

    public MultiMapMapReduceOperationFactory()
    {
    }

    public MultiMapMapReduceOperationFactory( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                      Reducer<KeyOut, ValueOut> reducer, boolean distributableReducer )
    {
        this.name = name;
        this.mapper = mapper;
        this.reducer = reducer;
        this.distributableReducer = distributableReducer;
    }

    @Override
    public void writeData( ObjectDataOutput out )
        throws IOException
    {
        out.writeUTF( name );
        out.writeObject( mapper );
        out.writeObject( reducer );
        out.writeBoolean( distributableReducer );
    }

    @Override
    public void readData( ObjectDataInput in )
        throws IOException
    {
        name = in.readUTF();
        mapper = in.readObject();
        reducer = in.readObject();
        distributableReducer = in.readBoolean();
    }

    @Override
    public Operation createOperation()
    {
        Reducer r = distributableReducer ? reducer : null;
        return new MultiMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, r );
    }

}
