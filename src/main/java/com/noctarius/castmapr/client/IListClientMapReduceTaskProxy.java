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

import java.util.Map;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.castmapr.core.AbstractMapReduceTask;
import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.MapReduceCollatorListener;
import com.noctarius.castmapr.spi.MapReduceListener;

public class IListClientMapReduceTaskProxy<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractMapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut>
{

    private final ClientContext context;

    public IListClientMapReduceTaskProxy( String name, ClientContext context, HazelcastInstance hazelcastInstance )
    {
        super( name, hazelcastInstance );
        this.context = context;
    }

    @Override
    protected Map<Integer, Object> invokeTasks( boolean distributableReducer )
        throws Exception
    {
        ClientInvocationService cis = context.getInvocationService();
        MapReduceRequest<KeyIn, ValueIn, KeyOut, ValueOut> request;
        request =
            new MapReduceRequest<KeyIn, ValueIn, KeyOut, ValueOut>( name, mapper, reducer,
                                                                    ClientMapReduceCollectionType.IList,
                                                                    distributableReducer );
        return cis.invokeOnRandomTarget( request );
    }

    @Override
    protected <R> MapReduceBackgroundTask<R> buildMapReduceBackgroundTask( MapReduceListener<KeyIn, ValueIn> listener )
    {
        return new ClientMapReduceBackgroundTask( listener );
    }

    @Override
    protected <R> MapReduceBackgroundTask<R> buildMapReduceBackgroundTask( Collator<KeyIn, ValueIn, R> collator,
                                                                           MapReduceCollatorListener<R> collatorListener )
    {
        return new ClientMapReduceBackgroundTask( collator, collatorListener );
    }

    @Override
    protected <R> void invokeAsyncTask( MapReduceBackgroundTask<R> task )
    {
        ClientExecutionService es = context.getExecutionService();
        es.execute( task );
    }

    private class ClientMapReduceBackgroundTask<R>
        extends MapReduceBackgroundTask<R>
    {

        private ClientMapReduceBackgroundTask( MapReduceListener<KeyIn, ValueIn> listener )
        {
            super( listener );
        }

        private ClientMapReduceBackgroundTask( Collator<KeyIn, ValueIn, R> collator,
                                               MapReduceCollatorListener<R> collatorListener )
        {
            super( collator, collatorListener );
        }

        @Override
        public void run()
        {
            ClientInvocationService cis = context.getInvocationService();
            try
            {
                MapReduceRequest<KeyIn, ValueIn, KeyOut, ValueOut> request;
                request =
                    new MapReduceRequest( name, mapper, reducer, ClientMapReduceCollectionType.IList,
                                          isDistributableReducer() );
                Map<Integer, Object> responses = cis.invokeOnRandomTarget( request );
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