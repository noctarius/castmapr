package com.noctarius.castmapr.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.noctarius.castmapr.core.MapReduceUtils;
import com.noctarius.castmapr.core.operation.IListMapReduceOperation;
import com.noctarius.castmapr.core.operation.IMapMapReduceOperation;
import com.noctarius.castmapr.core.operation.MapReduceOperationFactory;
import com.noctarius.castmapr.core.operation.MultiMapReduceOperation;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

public class KeyedMapReduceClientRequest<KeyIn, ValueIn, KeyOut, ValueOut>
    extends InvocationClientRequest
    implements DataSerializable
{

    protected String name;

    protected List<KeyIn> keys;

    protected Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    protected Reducer<KeyOut, ValueOut> reducer;

    protected ClientMapReduceCollectionType collectionType;

    protected boolean distributableReducer;

    protected KeyedMapReduceClientRequest()
    {
    }

    protected KeyedMapReduceClientRequest( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                           Reducer<KeyOut, ValueOut> reducer, List<KeyIn> keys,
                                           ClientMapReduceCollectionType collectionType, boolean distributableReducer )
    {
        this.name = name;
        this.keys = keys;
        this.mapper = mapper;
        this.reducer = reducer;
        this.collectionType = collectionType;
        this.distributableReducer = distributableReducer;
    }

    @Override
    protected void invoke()
    {
        MapReduceOperationFactory<KeyIn> factory = buildOperationFactory();

        ClientEndpoint endpoint = getEndpoint();
        ClientEngine clientEngine = (ClientEngine) getClientEngine();
        SerializationService ss = clientEngine.getSerializationService();
        PartitionService partitionService = clientEngine.getPartitionService();
        Map<Integer, List<KeyIn>> keyMapping = MapReduceUtils.mapKeysToPartitions( partitionService, keys );
        Map<Integer, Future<?>> futures = new HashMap<Integer, Future<?>>();
        for ( Entry<Integer, List<KeyIn>> entry : keyMapping.entrySet() )
        {
            Operation operation = factory.createOperation( entry.getKey(), entry.getValue() );
            InvocationBuilder inv = createInvocationBuilder( getServiceName(), operation, entry.getKey() );
            Invocation invocation = inv.build();
            futures.put( entry.getKey(), invocation.invoke() );
        }

        Map<Integer, Object> results = new HashMap<Integer, Object>();
        for ( Entry<Integer, Future<?>> entry : futures.entrySet() )
        {
            try
            {
                results.put( entry.getKey(), toObject( ss, entry.getValue().get() ) );
            }
            catch ( Throwable t )
            {
                results.put( entry.getKey(), t );
            }
        }

        List<Integer> failedPartitions = new LinkedList<Integer>();
        for ( Entry<Integer, Object> entry : results.entrySet() )
        {
            if ( entry.getValue() instanceof Throwable )
            {
                failedPartitions.add( entry.getKey() );
            }
        }

        for ( Integer partitionId : failedPartitions )
        {
            List<KeyIn> keys = keyMapping.get( partitionId );
            Operation operation = factory.createOperation( partitionId, keys );
            InvocationBuilder inv = createInvocationBuilder( getServiceName(), operation, partitionId );
            Invocation invocation = inv.build();
            results.put( partitionId, invocation.invoke() );
        }

        for ( Integer failedPartition : failedPartitions )
        {
            try
            {
                Future<?> future = (Future<?>) results.get( failedPartition );
                Object result = future.get();
                results.put( failedPartition, result );
            }
            catch ( Throwable t )
            {
                results.put( failedPartition, t );
            }
        }

        clientEngine.sendResponse( endpoint, results );
    }

    @Override
    public String getServiceName()
    {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void writeData( ObjectDataOutput out )
        throws IOException
    {
        out.writeObject( mapper );
        out.writeObject( reducer );
        out.writeInt( keys == null ? 0 : keys.size() );
        for ( KeyIn key : keys )
        {
            out.writeObject( key );
        }
        out.writeByte( collectionType.ordinal() );
        out.writeBoolean( distributableReducer );
    }

    @Override
    public void readData( ObjectDataInput in )
        throws IOException
    {
        mapper = in.readObject();
        reducer = in.readObject();
        int size = in.readInt();
        keys = new ArrayList<KeyIn>( size );
        for ( int i = 0; i < size; i++ )
        {
            keys.add( (KeyIn) in.readObject() );
        }
        collectionType = ClientMapReduceCollectionType.byOrdinal( in.readByte() );
        distributableReducer = in.readBoolean();
    }

    protected MapReduceOperationFactory<KeyIn> buildOperationFactory()
    {
        final Reducer r = distributableReducer ? reducer : null;
        switch ( collectionType )
        {
            case IMap:
                return new MapReduceOperationFactory<KeyIn>()
                {

                    @Override
                    public Operation createOperation( int partitionId, List<KeyIn> keys )
                    {
                        // TODO
                        return new IMapMapReduceOperation( getServiceName(), mapper, reducer );
                    }
                };
            case MultiMap:
                return new MapReduceOperationFactory<KeyIn>()
                {

                    @Override
                    public Operation createOperation( int partitionId, List<KeyIn> keys )
                    {
                        // TODO
                        return new MultiMapReduceOperation( getServiceName(), mapper, reducer );
                    }
                };
            case IList:
                return new MapReduceOperationFactory<KeyIn>()
                {

                    @Override
                    public Operation createOperation( int partitionId, List<KeyIn> keys )
                    {
                        // TODO
                        return new IListMapReduceOperation( getServiceName(), mapper, reducer );
                    }
                };
            default:
                throw new IllegalStateException( "Illegal ClientMapReduceCollectionType was found" );
        }
    }

    private Object toObject( SerializationService ss, Object value )
    {
        if ( value instanceof Data )
        {
            return ss.toObject( (Data) value );
        }
        return value;
    }

}
