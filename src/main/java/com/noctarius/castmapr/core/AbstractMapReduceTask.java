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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.castmapr.MapReduceTask;
import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.Distributable;
import com.noctarius.castmapr.spi.DistributableReducer;
import com.noctarius.castmapr.spi.MapReduceCollatorListener;
import com.noctarius.castmapr.spi.MapReduceListener;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

public abstract class AbstractMapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut>
    implements MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut>
{

    protected final String name;

    protected final HazelcastInstance hazelcastInstance;

    protected Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    protected Reducer<KeyOut, ValueOut> reducer;

    public AbstractMapReduceTask( String name, HazelcastInstance hazelcastInstance )
    {
        this.name = name;
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public MapReduceTask<KeyOut, List<ValueOut>, KeyOut, ValueOut> mapper( Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper )
    {
        if ( mapper == null )
            throw new IllegalStateException( "mapper must not be null" );
        if ( this.mapper != null )
            throw new IllegalStateException( "mapper already set" );
        this.mapper = mapper;
        return (MapReduceTask<KeyOut, List<ValueOut>, KeyOut, ValueOut>) this;
    }

    @Override
    public MapReduceTask<KeyOut, ValueOut, KeyOut, ValueOut> reducer( Reducer<KeyOut, ValueOut> reducer )
    {
        if ( reducer == null )
            throw new IllegalStateException( "reducer must not be null" );
        if ( this.reducer != null )
            throw new IllegalStateException( "reducer already set" );
        this.reducer = reducer;
        return (MapReduceTask<KeyOut, ValueOut, KeyOut, ValueOut>) this;
    }

    @Override
    public Map<KeyIn, ValueIn> submit()
    {
        try
        {

            Map<Integer, Object> responses = invokeTasks( isDistributableReducer() );
            Map<KeyOut, List<ValueOut>> groupedResponses = groupResponsesByKey( responses );
            return (Map<KeyIn, ValueIn>) finalReduceStep( groupedResponses );
        }
        catch ( Throwable t )
        {
            ExceptionUtil.rethrow( t );
        }
        return Collections.emptyMap();
    }

    @Override
    public <R> R submit( Collator<KeyIn, ValueIn, R> collator )
    {
        Map<KeyIn, ValueIn> reducedResults = submit();
        return collator.collate( reducedResults );
    }

    @Override
    public void submitAsync( MapReduceListener<KeyIn, ValueIn> listener )
    {
        MapReduceBackgroundTask<?> task = buildMapReduceBackgroundTask( listener );
        invokeAsyncTask( task );
    }

    @Override
    public <R> void submitAsync( Collator<KeyIn, ValueIn, R> collator, MapReduceCollatorListener<R> listener )
    {
        MapReduceBackgroundTask<R> task = buildMapReduceBackgroundTask( collator, listener );
        invokeAsyncTask( task );
    }

    @Override
    public void submitAsync( MapReduceListener<KeyIn, ValueIn> listener, ExecutorService executorService )
    {
        MapReduceBackgroundTask<?> task = buildMapReduceBackgroundTask( listener );
        executorService.execute( task );
    }

    @Override
    public <R> void submitAsync( Collator<KeyIn, ValueIn, R> collator, MapReduceCollatorListener<R> listener,
                                 ExecutorService executorService )
    {
        MapReduceBackgroundTask<R> task = buildMapReduceBackgroundTask( collator, listener );
        executorService.execute( task );
    }

    protected Map<KeyOut, ValueOut> finalReduceStep( Map<KeyOut, List<ValueOut>> groupedResponses )
    {
        Map<KeyOut, ValueOut> reducedResults = new HashMap<KeyOut, ValueOut>();

        if ( reducer instanceof HazelcastInstanceAware )
        {
            ( (HazelcastInstanceAware) reducer ).setHazelcastInstance( hazelcastInstance );
        }

        // Final local reduce step
        for ( Entry<KeyOut, List<ValueOut>> entry : groupedResponses.entrySet() )
        {
            if ( isDistributableReducer() )
            {
                reducedResults.put( entry.getKey(), reducer.reduce( entry.getKey(), entry.getValue().iterator() ) );
            }
            else
            {
                List results = new ArrayList( groupedResponses.size() );
                for ( Object value : prepareIntermediateResults( entry.getValue() ) )
                {
                    // Eventually aggregate subresults to one big result list
                    if ( value instanceof List )
                    {
                        for ( Object innerValue : ( (List) value ) )
                        {
                            results.add( innerValue );
                        }
                    }
                    else
                    {
                        results.add( value );
                    }
                }
                if ( reducer != null )
                {
                    reducedResults.put( entry.getKey(), reducer.reduce( entry.getKey(), results.iterator() ) );
                }
                else
                {
                    reducedResults.put( entry.getKey(), (ValueOut) results );
                }
            }
        }
        return reducedResults;
    }

    protected List<ValueOut> prepareIntermediateResults( Object value )
    {
        // If reducer was just not distributable collect intermediate results
        if ( reducer != null && !isDistributableReducer() && value instanceof List )
        {
            List<ValueOut> intermediateResults = new ArrayList<ValueOut>();
            for ( Object list : ( (List) value ) )
            {
                if ( list instanceof List )
                {
                    for ( Object innerValue : (List) list )
                    {
                        intermediateResults.add( (ValueOut) innerValue );
                    }
                }
                else
                {
                    intermediateResults.add( (ValueOut) value );
                }
            }
            return intermediateResults;
        }
        return (List<ValueOut>) value;
    }

    protected Map<KeyOut, List<ValueOut>> groupResponsesByKey( Map<Integer, Object> responses )
    {
        Map<KeyOut, List<ValueOut>> groupedResponses = new HashMap<KeyOut, List<ValueOut>>();
        for ( Entry<Integer, Object> entry : responses.entrySet() )
        {
            Map<KeyOut, ValueOut> resultMap = (Map<KeyOut, ValueOut>) entry.getValue();
            for ( Entry<KeyOut, ValueOut> resultMapEntry : resultMap.entrySet() )
            {
                List<ValueOut> list = groupedResponses.get( resultMapEntry.getKey() );
                if ( list == null )
                {
                    list = new LinkedList<ValueOut>();
                    groupedResponses.put( resultMapEntry.getKey(), list );
                }
                list.add( resultMapEntry.getValue() );
            }
        }
        return groupedResponses;
    }

    protected boolean isDistributableReducer()
    {
        if ( reducer == null )
        {
            return false;
        }

        Class<? extends Reducer> clazz = reducer.getClass();
        if ( clazz.isAnnotationPresent( Distributable.class ) )
        {
            return true;
        }

        return DistributableReducer.class.isAssignableFrom( clazz );
    }

    protected abstract Map<Integer, Object> invokeTasks( boolean distributableReducer )
        throws Exception;

    protected abstract <R> MapReduceBackgroundTask<R> buildMapReduceBackgroundTask( MapReduceListener<KeyIn, ValueIn> listener );

    protected abstract <R> MapReduceBackgroundTask<R> buildMapReduceBackgroundTask( Collator<KeyIn, ValueIn, R> collator,
                                                                                    MapReduceCollatorListener<R> collatorListener );

    protected abstract <R> void invokeAsyncTask( MapReduceBackgroundTask<R> task );

    protected abstract class MapReduceBackgroundTask<R>
        implements Runnable
    {

        protected final MapReduceListener<KeyIn, ValueIn> listener;

        protected final MapReduceCollatorListener<R> collatorListener;

        protected final Collator<KeyIn, ValueIn, R> collator;

        protected MapReduceBackgroundTask( MapReduceListener<KeyIn, ValueIn> listener )
        {
            this.listener = listener;
            this.collator = null;
            this.collatorListener = null;
        }

        protected MapReduceBackgroundTask( Collator<KeyIn, ValueIn, R> collator,
                                           MapReduceCollatorListener<R> collatorListener )
        {
            this.collator = collator;
            this.collatorListener = collatorListener;
            this.listener = null;
        }
    }
}
