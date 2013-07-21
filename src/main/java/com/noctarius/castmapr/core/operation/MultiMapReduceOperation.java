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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.CollectionWrapper;
import com.hazelcast.core.MultiMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.noctarius.castmapr.core.CollectorImpl;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.MultiMapAware;
import com.noctarius.castmapr.spi.PartitionIdAware;
import com.noctarius.castmapr.spi.Reducer;

public class MultiMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
{

    public MultiMapReduceOperation()
    {
    }

    public MultiMapReduceOperation( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                    Reducer<KeyOut, ValueOut> reducer, List<KeyIn> keys )
    {
        super( name, mapper, reducer, keys );
    }

    @Override
    public void run()
        throws Exception
    {
        CollectionService service = ( (NodeEngineImpl) getNodeEngine() ).getService( CollectionService.SERVICE_NAME );
        CollectionContainer container = getCollectionContainer( service );

        ProxyService proxyService = getNodeEngine().getProxyService();
        MultiMap<KeyIn, ValueIn> multiMap = getMultiMap( proxyService );

        int partitionId = getPartitionId();
        if ( mapper instanceof PartitionIdAware )
        {
            ( (PartitionIdAware) mapper ).setPartitionId( partitionId );
        }
        if ( mapper instanceof MultiMapAware )
        {
            ( (MultiMapAware) mapper ).setMultiMap( multiMap );
        }

        CollectorImpl<KeyOut, ValueOut> collector = new CollectorImpl<KeyOut, ValueOut>();

        mapper.initialize( collector );
        // Without defined keys iterate all keys
        if ( keys == null || keys.size() == 0 )
        {
            for ( Data dataKey : container.keySet() )
            {
                KeyIn key = (KeyIn) getNodeEngine().toObject( dataKey );
                CollectionWrapper collectionWrapper = container.getCollectionWrapper( dataKey );
                if ( collectionWrapper != null )
                {
                    Collection<CollectionRecord> collection = collectionWrapper.getCollection();
                    for ( CollectionRecord record : collection )
                    {
                        ValueIn value = (ValueIn) getNodeEngine().toObject( record.getObject() );
                        mapper.map( key, value, collector );
                    }
                }
            }
        }
        else
        {
            // Iterate only defined keys
            for ( KeyIn key : keys )
            {
                Data dataKey = getNodeEngine().toData( key );
                CollectionWrapper collectionWrapper = container.getCollectionWrapper( dataKey );
                if ( collectionWrapper != null )
                {
                    Collection<CollectionRecord> collection = collectionWrapper.getCollection();
                    for ( CollectionRecord record : collection )
                    {
                        ValueIn value = (ValueIn) getNodeEngine().toObject( record.getObject() );
                        mapper.map( key, value, collector );
                    }
                }
            }
        }
        mapper.finalized( collector );

        if ( reducer != null )
        {
            if ( reducer instanceof PartitionIdAware )
            {
                ( (PartitionIdAware) reducer ).setPartitionId( partitionId );
            }
            if ( reducer instanceof MultiMapAware )
            {
                ( (MultiMapAware) reducer ).setMultiMap( multiMap );
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

    private MultiMap<KeyIn, ValueIn> getMultiMap( ProxyService proxyService )
    {
        return (MultiMap) proxyService.getDistributedObject( CollectionService.SERVICE_NAME,
                                                             new CollectionProxyId( name, null,
                                                                                    CollectionProxyType.MULTI_MAP ) );
    }

    private CollectionContainer getCollectionContainer( CollectionService collectionService )
    {
        CollectionProxyId proxyId = new CollectionProxyId( name, null, CollectionProxyType.MULTI_MAP );
        return collectionService.getOrCreateCollectionContainer( getPartitionId(), proxyId );
    }
}
