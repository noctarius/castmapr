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
import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.core.IList;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.noctarius.castmapr.core.CollectorImpl;
import com.noctarius.castmapr.spi.IListAware;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.PartitionIdAware;
import com.noctarius.castmapr.spi.Reducer;

public class IListMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractNamedOperation
    implements PartitionAwareOperation
{

    private Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    private Reducer<KeyOut, ValueOut> reducer;

    private transient Object response;

    public IListMapReduceOperation()
    {
    }

    public IListMapReduceOperation( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
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
        CollectionService service = ( (NodeEngineImpl) getNodeEngine() ).getService( CollectionService.SERVICE_NAME );
        CollectionContainer container = getCollectionContainer( service );

        ProxyService proxyService = getNodeEngine().getProxyService();
        IList<ValueIn> list = getList( proxyService );

        int partitionId = getPartitionId();
        if ( mapper instanceof PartitionIdAware )
        {
            ( (PartitionIdAware) mapper ).setPartitionId( partitionId );
        }
        if ( mapper instanceof IListAware )
        {
            ( (IListAware) mapper ).setMultiMap( list );
        }

        Data dataKey = getNodeEngine().toData( name );
        CollectionWrapper collectionWrapper = container.getOrCreateCollectionWrapper( dataKey );
        Collection<CollectionRecord> collection = collectionWrapper.getCollection();

        CollectorImpl<KeyOut, ValueOut> collector = new CollectorImpl<KeyOut, ValueOut>();

        mapper.initialize( collector );
        for ( CollectionRecord record : collection )
        {
            ValueIn value = (ValueIn) getNodeEngine().toObject( record.getObject() );
            mapper.map( null, value, collector );
        }
        mapper.finalized( collector );

        if ( reducer != null )
        {
            if ( reducer instanceof PartitionIdAware )
            {
                ( (PartitionIdAware) reducer ).setPartitionId( partitionId );
            }
            if ( reducer instanceof IListAware )
            {
                ( (IListAware) reducer ).setMultiMap( list );
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
    public boolean returnsResponse()
    {
        return true;
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

    private IList<ValueIn> getList( ProxyService proxyService )
    {
        CollectionProxyId proxyId =
            new CollectionProxyId( ObjectListProxy.COLLECTION_LIST_NAME, name, CollectionProxyType.LIST );
        return (IList<ValueIn>) proxyService.getDistributedObject( CollectionService.SERVICE_NAME, proxyId );
    }

    private CollectionContainer getCollectionContainer( CollectionService collectionService )
    {
        CollectionProxyId proxyId =
            new CollectionProxyId( ObjectListProxy.COLLECTION_LIST_NAME, name, CollectionProxyType.LIST );
        return collectionService.getOrCreateCollectionContainer( getPartitionId(), proxyId );
    }
}
