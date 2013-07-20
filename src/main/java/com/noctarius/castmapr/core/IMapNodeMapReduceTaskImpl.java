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

package com.noctarius.castmapr.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.castmapr.core.operation.IMapMapReduceOperation;
import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.MapReduceCollatorListener;
import com.noctarius.castmapr.spi.MapReduceListener;
import com.noctarius.castmapr.spi.Reducer;

import static com.noctarius.castmapr.core.MapReduceUtils.*;

public class IMapNodeMapReduceTaskImpl<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractMapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut>
{

    private final NodeEngine nodeEngine;

    public IMapNodeMapReduceTaskImpl( String name, NodeEngine nodeEngine, HazelcastInstance hazelcastInstance )
    {
        super( name, hazelcastInstance );
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected Map<Integer, Object> invokeTasks( Iterable<KeyIn> keys, boolean distributableReducer )
        throws Exception
    {
        OperationService os = nodeEngine.getOperationService();
        PartitionService partitionService = nodeEngine.getPartitionService();
        SerializationService ss = nodeEngine.getSerializationService();

        Reducer r = distributableReducer ? reducer : null;
        Map<Integer, List<KeyIn>> keyMapping = mapKeysToPartitions( partitionService, (List<KeyIn>) copyKeys( keys ) );
        Map<Integer, Future<?>> futures = new HashMap<Integer, Future<?>>();
        for ( Entry<Integer, List<KeyIn>> entry : keyMapping.entrySet() )
        {
            Operation operation = new IMapMapReduceOperation( name, mapper, r, entry.getValue() );
            operation.setNodeEngine( nodeEngine ).setCallerUuid( nodeEngine.getLocalMember().getUuid() );
            InvocationBuilder inv = os.createInvocationBuilder( MapService.SERVICE_NAME, operation, entry.getKey() );
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
            Operation operation = new IMapMapReduceOperation( name, mapper, r, keyMapping.get( partitionId ) );
            InvocationBuilder inv = os.createInvocationBuilder( MapService.SERVICE_NAME, operation, partitionId );
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
        return results;
    }

    @Override
    protected Map<Integer, Object> invokeTasks( boolean distributableReducer )
        throws Exception
    {
        OperationService os = nodeEngine.getOperationService();

        Reducer r = distributableReducer ? reducer : null;
        IMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut> operation;
        operation = new IMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, r, null );
        operation.setNodeEngine( nodeEngine ).setCallerUuid( nodeEngine.getLocalMember().getUuid() );
        return os.invokeOnAllPartitions( MapService.SERVICE_NAME, new BinaryOperationFactory( operation, nodeEngine ) );
    }

    @Override
    protected <R> MapReduceBackgroundTask<R> buildMapReduceBackgroundTask( Iterable<KeyIn> keys,
                                                                           MapReduceListener<KeyIn, ValueIn> listener )
    {
        return new NodeMapReduceBackgroundTask( keys, listener );
    }

    @Override
    protected <R> MapReduceBackgroundTask<R> buildMapReduceBackgroundTask( Iterable<KeyIn> keys,
                                                                           Collator<KeyIn, ValueIn, R> collator,
                                                                           MapReduceCollatorListener<R> collatorListener )
    {
        return new NodeMapReduceBackgroundTask<R>( keys, collator, collatorListener );
    }

    @Override
    protected <R> void invokeAsyncTask( MapReduceBackgroundTask<R> task )
    {
        ExecutorService es = nodeEngine.getExecutionService().getExecutor( "hz:query" );
        es.execute( task );
    }

    private class NodeMapReduceBackgroundTask<R>
        extends MapReduceBackgroundTask<R>
    {

        private NodeMapReduceBackgroundTask( Iterable<KeyIn> keys, MapReduceListener<KeyIn, ValueIn> listener )
        {
            super( keys, listener );
        }

        private NodeMapReduceBackgroundTask( Iterable<KeyIn> keys, Collator<KeyIn, ValueIn, R> collator,
                                             MapReduceCollatorListener<R> collatorListener )
        {
            super( keys, collator, collatorListener );
        }

        @Override
        public void run()
        {
            OperationService os = nodeEngine.getOperationService();
            Reducer r = isDistributableReducer() ? reducer : null;
            IMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut> operation;
            operation = new IMapMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, r, null );
            operation.setNodeEngine( nodeEngine ).setCallerUuid( nodeEngine.getLocalMember().getUuid() );
            try
            {
                Map<Integer, Object> responses;
                if ( keys != null )
                {
                    PartitionService ps = nodeEngine.getPartitionService();
                    Set<Integer> partitions = new HashSet<Integer>();
                    for ( KeyIn key : keys )
                    {
                        partitions.add( ps.getPartitionId( key ) );
                    }
                    responses =
                        os.invokeOnPartitions( MapService.SERVICE_NAME, new BinaryOperationFactory( operation,
                                                                                                    nodeEngine ),
                                               partitions );
                }
                else
                {
                    responses =
                        os.invokeOnAllPartitions( MapService.SERVICE_NAME, new BinaryOperationFactory( operation,
                                                                                                       nodeEngine ) );
                }

                Map groupedResponses = groupResponsesByKey( responses );
                Map reducedResults = finalReduceStep( groupedResponses );
                if ( collator == null )
                {
                    listener.onCompletion( reducedResults );
                }
                else
                {
                    R result = collator.collate( reducedResults );
                    collatorListener.onCompletion( result );
                }
            }
            catch ( Throwable t )
            {
                ExceptionUtil.rethrow( t );
            }
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
