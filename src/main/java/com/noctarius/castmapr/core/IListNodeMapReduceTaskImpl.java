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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.castmapr.core.operation.IListMapReduceOperation;
import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.MapReduceCollatorListener;
import com.noctarius.castmapr.spi.MapReduceListener;
import com.noctarius.castmapr.spi.Reducer;

public class IListNodeMapReduceTaskImpl<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractMapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut>
{

    private final NodeEngine nodeEngine;

    public IListNodeMapReduceTaskImpl( String name, NodeEngine nodeEngine, HazelcastInstance hazelcastInstance )
    {
        super( name, hazelcastInstance );
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected Map<Integer, Object> invokeTasks( Iterable<KeyIn> keys, boolean distributableReducer )
        throws Exception
    {
        OperationService os = nodeEngine.getOperationService();

        Reducer r = distributableReducer ? reducer : null;
        IListMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut> operation;
        operation = new IListMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, r );
        operation.setNodeEngine( nodeEngine ).setCallerUuid( nodeEngine.getLocalMember().getUuid() );
        PartitionService ps = nodeEngine.getPartitionService();
        Set<Integer> partitions = new HashSet<Integer>();
        for ( KeyIn key : keys )
        {
            partitions.add( ps.getPartitionId( key ) );
        }
        return os.invokeOnPartitions( MapService.SERVICE_NAME, new BinaryOperationFactory( operation, nodeEngine ),
                                      partitions );
    }

    @Override
    protected Map<Integer, Object> invokeTasks( boolean distributableReducer )
        throws Exception
    {
        OperationService os = nodeEngine.getOperationService();

        Reducer r = distributableReducer ? reducer : null;
        IListMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut> operation;
        operation = new IListMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, r );
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
            try
            {
                Map<Integer, Object> responses = invokeTasks( isDistributableReducer() );
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

}
