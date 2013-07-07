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
import com.hazelcast.core.MultiMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.noctarius.castmapr.core.CollectorImpl;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.MultiMapAware;
import com.noctarius.castmapr.spi.PartitionIdAware;
import com.noctarius.castmapr.spi.Reducer;

public class MultiMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractNamedOperation
    implements PartitionAwareOperation
{

    private Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    private Reducer<KeyOut, ValueOut> reducer;

    private transient Object response;

    public MultiMapMapReduceOperation()
    {
    }

    public MultiMapMapReduceOperation( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
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
        for ( Data dataKey : container.keySet() )
        {
            KeyIn key = (KeyIn) getNodeEngine().toObject( dataKey );
            CollectionWrapper collectionWrapper = container.getCollectionWrapper( dataKey );
            Collection<CollectionRecord> collection = collectionWrapper.getCollection();
            for ( CollectionRecord record : collection )
            {
                ValueIn value = (ValueIn) getNodeEngine().toObject( record.getObject() );
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

    @Override
    public boolean returnsResponse()
    {
        return true;
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
