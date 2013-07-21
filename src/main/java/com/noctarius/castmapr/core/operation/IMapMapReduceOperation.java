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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.core.IMap;
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.noctarius.castmapr.core.CollectorImpl;
import com.noctarius.castmapr.spi.MapAware;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.PartitionIdAware;
import com.noctarius.castmapr.spi.Reducer;

public class IMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
{

    protected transient MapService mapService;

    protected transient MapContainer mapContainer;

    public IMapMapReduceOperation()
    {
    }

    public IMapMapReduceOperation( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                   Reducer<KeyOut, ValueOut> reducer, List<KeyIn> keys )
    {
        super( name, mapper, reducer, keys );
    }

    @Override
    public void beforeRun()
        throws Exception
    {
        mapService = getService();
        mapContainer = mapService.getMapContainer( name );
        if ( !( this instanceof BackupOperation ) && !mapContainer.isMapReady() )
        {
            throw new RetryableHazelcastException( "Map is not ready." );
        }
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
        // Without defined keys iterate all keys
        if ( keys.size() == 0 )
        {
            for ( Entry<Data, Data> entry : recordStore.entrySetData() )
            {
                KeyIn key = (KeyIn) mapService.toObject( entry.getKey() );
                ValueIn value = (ValueIn) mapService.toObject( entry.getValue() );
                mapper.map( key, value, collector );
            }
        }
        else
        {
            // Iterate only defined keys
            for ( KeyIn key : keys )
            {
                Data dataKey = mapService.toData( key );
                ValueIn value = (ValueIn) mapService.toObject( recordStore.get( dataKey ) );
                mapper.map( key, value, collector );
            }
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

}
