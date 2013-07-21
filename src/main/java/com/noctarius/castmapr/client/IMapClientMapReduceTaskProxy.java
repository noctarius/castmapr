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

import static com.noctarius.castmapr.core.MapReduceUtils.copyKeys;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.castmapr.core.AbstractMapReduceTask;
import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.KeyPredicate;
import com.noctarius.castmapr.spi.MapReduceCollatorListener;
import com.noctarius.castmapr.spi.MapReduceListener;

public class IMapClientMapReduceTaskProxy<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractMapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut>
{

    private final ClientContext context;

    public IMapClientMapReduceTaskProxy( String name, ClientContext context, HazelcastInstance hazelcastInstance )
    {
        super( name, hazelcastInstance );
        this.context = context;
    }

    @Override
    protected Map<Integer, Object> invokeTasks( Iterable<KeyIn> keys, boolean distributableReducer )
        throws Exception
    {
        ClientInvocationService cis = context.getInvocationService();
        Object request =
            new KeyedMapReduceClientRequest( name, mapper, reducer, (List<KeyIn>) copyKeys( keys ),
                                             ClientMapReduceCollectionType.IMap, distributableReducer );
        return cis.invokeOnRandomTarget( request );
    }

    @Override
    protected Map<Integer, Object> invokeTasks( boolean distributableReducer )
        throws Exception
    {
        ClientInvocationService cis = context.getInvocationService();
        AllKeysMapReduceRequest<KeyIn, ValueIn, KeyOut, ValueOut> request;
        request =
            new AllKeysMapReduceRequest( name, mapper, reducer, ClientMapReduceCollectionType.IMap,
                                         distributableReducer );
        return cis.invokeOnRandomTarget( request );
    }

    @Override
    protected <R> MapReduceBackgroundTask<R> buildMapReduceBackgroundTask( Iterable<KeyIn> keys,
                                                                           MapReduceListener<KeyIn, ValueIn> listener )
    {
        return new ClientMapReduceBackgroundTask( keys, listener );
    }

    @Override
    protected <R> MapReduceBackgroundTask<R> buildMapReduceBackgroundTask( Iterable<KeyIn> keys,
                                                                           Collator<KeyIn, ValueIn, R> collator,
                                                                           MapReduceCollatorListener<R> collatorListener )
    {
        return new ClientMapReduceBackgroundTask( keys, collator, collatorListener );
    }

    @Override
    protected List<KeyIn> evaluateKeys( KeyPredicate<KeyIn> predicate )
    {
        IMap<KeyIn, ValueIn> map = hazelcastInstance.getMap( name );
        Set<KeyIn> keys = map.keySet();
        List<KeyIn> result = new ArrayList<KeyIn>();
        for ( KeyIn key : keys )
        {
            if ( predicate.evaluate( key ) )
            {
                result.add( key );
            }
        }
        return result.size() > 0 ? result : null;
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

        private ClientMapReduceBackgroundTask( Iterable<KeyIn> keys, MapReduceListener<KeyIn, ValueIn> listener )
        {
            super( keys, listener );
        }

        private ClientMapReduceBackgroundTask( Iterable<KeyIn> keys, Collator<KeyIn, ValueIn, R> collator,
                                               MapReduceCollatorListener<R> collatorListener )
        {
            super( keys, collator, collatorListener );
        }

        @Override
        public void run()
        {
            ClientInvocationService cis = context.getInvocationService();
            try
            {
                Object request;
                if ( keys == null )
                {
                    request =
                        new AllKeysMapReduceRequest( name, mapper, reducer, ClientMapReduceCollectionType.IMap,
                                                     isDistributableReducer() );
                }
                else
                {
                    request =
                        new KeyedMapReduceClientRequest( name, mapper, reducer, (List<KeyIn>) copyKeys( keys ),
                                                         ClientMapReduceCollectionType.IMap, isDistributableReducer() );
                }
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
