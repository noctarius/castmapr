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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.core.IMap;
import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.operation.AbstractMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ProxyService;
import com.noctarius.castmapr.core.CollectorImpl;
import com.noctarius.castmapr.spi.MapAware;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.PartitionIdAware;
import com.noctarius.castmapr.spi.Reducer;

public class IMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractMapOperation
    implements PartitionAwareOperation
{

    private Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    private Reducer<KeyOut, ValueOut> reducer;

    private transient Object response;

    public IMapMapReduceOperation()
    {
    }

    public IMapMapReduceOperation( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                               Reducer<KeyOut, ValueOut> reducer )
    {
        super( name );
        this.mapper = mapper;
        this.reducer = reducer;
    }

    @Override
    public void run()
        throws Exception
    {
        int partitionId = getPartitionId();
        ProxyService proxyService = getNodeEngine().getProxyService();
        if ( mapper instanceof PartitionIdAware )
        {
            ( (PartitionIdAware) mapper ).setPartitionId( partitionId );
        }
        if ( mapper instanceof MapAware )
        {
            IMap map = (IMap) proxyService.getDistributedObject( MapService.SERVICE_NAME, name );
            ( (MapAware) mapper ).setMap( map );
        }

        CollectorImpl<KeyOut, ValueOut> collector = new CollectorImpl<KeyOut, ValueOut>();
        RecordStore recordStore = mapService.getRecordStore( partitionId, name );

        mapper.initialize( collector );
        for ( Entry<Data, Data> entry : recordStore.entrySetData() )
        {
            KeyIn key = (KeyIn) mapService.toObject( entry.getKey() );
            ValueIn value = (ValueIn) mapService.toObject( entry.getValue() );
            mapper.map( key, value, collector );
        }
        mapper.finalized( collector );

        if ( reducer != null )
        {
            if ( reducer instanceof PartitionIdAware )
            {
                ( (PartitionIdAware) reducer ).setPartitionId( partitionId );
            }
            if ( reducer instanceof MapAware )
            {
                IMap map = (IMap) proxyService.getDistributedObject( MapService.SERVICE_NAME, name );
                ( (MapAware) reducer ).setMap( map );
            }
            Map<KeyOut, ValueOut> reducedResults = new HashMap<KeyOut, ValueOut>( collector.emitted.keySet().size() );
            for ( Entry<KeyOut, List<ValueOut>> entry : collector.emitted.entrySet() )
            {
                reducedResults.put( entry.getKey(), reducer.reduce( entry.getKey(), entry.getValue().iterator() ) );
            }
            response = reducedResults;
        }
        else
        {
            response = collector.emitted;
        }
    }

    @Override
    public Object getResponse()
    {
        return response;
    }

    @Override
    protected void writeInternal( ObjectDataOutput out )
        throws IOException
    {
        super.writeInternal( out );
        out.writeObject( mapper );
        out.writeObject( reducer );
    }

    @Override
    protected void readInternal( ObjectDataInput in )
        throws IOException
    {
        super.readInternal( in );
        mapper = in.readObject();
        reducer = in.readObject();
    }

}
