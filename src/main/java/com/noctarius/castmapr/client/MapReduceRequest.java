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

package com.noctarius.castmapr.client;

import java.io.IOException;
import java.util.Map;

import com.hazelcast.client.AllPartitionsClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.OperationFactory;
import com.noctarius.castmapr.core.operation.IListMapReduceOperationFactory;
import com.noctarius.castmapr.core.operation.IMapMapReduceOperationFactory;
import com.noctarius.castmapr.core.operation.MultiMapMapReduceOperationFactory;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

public class MapReduceRequest<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AllPartitionsClientRequest
    implements DataSerializable, RetryableRequest
{

    private ClientMapReduceCollectionType collectionType;

    private Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    private Reducer<KeyOut, ValueOut> reducer;

    private boolean distributableReducer;

    private String name;

    public MapReduceRequest()
    {
    }

    public MapReduceRequest( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                             Reducer<KeyOut, ValueOut> reducer, ClientMapReduceCollectionType collectionType,
                             boolean distributableReducer )
    {
        this.name = name;
        this.mapper = mapper;
        this.reducer = reducer;
        this.collectionType = collectionType;
        this.distributableReducer = distributableReducer;
    }

    @Override
    public void writeData( ObjectDataOutput out )
        throws IOException
    {
        out.writeUTF( name );
        out.writeObject( mapper );
        out.writeObject( reducer );
        out.writeByte( collectionType.ordinal() );
        out.writeBoolean( distributableReducer );
    }

    @Override
    public void readData( ObjectDataInput in )
        throws IOException
    {
        name = in.readUTF();
        mapper = in.readObject();
        reducer = in.readObject();
        collectionType = ClientMapReduceCollectionType.byOrdinal( in.readByte() );
        distributableReducer = in.readBoolean();
    }

    @Override
    protected OperationFactory createOperationFactory()
    {
        switch ( collectionType )
        {
            case IMap:
                return new IMapMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, reducer,
                                                                                            distributableReducer );
            case MultiMap:
                return new MultiMapMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, reducer,
                                                                                                distributableReducer );
            case IList:
                return new IListMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, reducer,
                                                                                             distributableReducer );
            default:
                throw new IllegalStateException( "Illegal ClientMapReduceCollectionType was found" );
        }
    }

    @Override
    protected Object reduce( Map<Integer, Object> map )
    {
        return map;
    }

    @Override
    public String getServiceName()
    {
        return MapService.SERVICE_NAME;
    }

}
