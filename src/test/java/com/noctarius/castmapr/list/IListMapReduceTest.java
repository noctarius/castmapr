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

package com.noctarius.castmapr.list;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SerialTest;
import com.noctarius.castmapr.AbstractMapReduceTaskTest;
import com.noctarius.castmapr.MapReduceTask;
import com.noctarius.castmapr.MapReduceTaskFactory;
import com.noctarius.castmapr.AbstractMapReduceTaskTest.CountingAware;
import com.noctarius.castmapr.AbstractMapReduceTaskTest.CountingManagedContext;
import com.noctarius.castmapr.MapReduceTest.TestMapper;
import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.Collector;
import com.noctarius.castmapr.spi.Distributable;
import com.noctarius.castmapr.spi.MapReduceCollatorListener;
import com.noctarius.castmapr.spi.MapReduceListener;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

@RunWith( HazelcastJUnit4ClassRunner.class )
@Category( SerialTest.class )
@SuppressWarnings( "unused" )
public class IListMapReduceTest
    extends AbstractMapReduceTaskTest
{

    private static final String LIST_NAME = "default";

    @Before
    public void gc()
    {
        Runtime.getRuntime().gc();
    }

    @After
    public void cleanup()
    {
        Hazelcast.shutdownAll();
    }

    @Test( timeout = 30000 )
    public void testListMapper()
        throws Exception
    {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory( 4 );
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext( context );

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance( config );
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance( config );
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance( config );

        IList<Integer> m1 = h1.getList( LIST_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.add( i );
        }

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( h1 );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        Map<String, List<Integer>> result = task.mapper( new TestMapper() ).submit();
        assertEquals( 1, result.size() );
        for ( List<Integer> value : result.values() )
        {
            assertEquals( 100, value.size() );
        }

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals( 0, hazelcastNames.size() );
    }

    @Test( timeout = 30000 )
    public void testListMapperReducer()
        throws Exception
    {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory( 4 );
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext( context );

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance( config );
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance( config );
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance( config );

        int expectedResult = 0;
        IList<Integer> m1 = h1.getList( LIST_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.add( i );
            expectedResult += i;
        }

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( h1 );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        Map<String, Integer> result = task.mapper( new TestMapper() ).reducer( new TestReducer() ).submit();

        assertEquals( 1, result.size() );
        for ( Integer value : result.values() )
        {
            assertEquals( expectedResult, (int) value );
        }

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals( 0, hazelcastNames.size() );
    }

    @Test( timeout = 30000 )
    public void testListMapperDistributedReducer()
        throws Exception
    {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory( 4 );
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext( context );

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance( config );
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance( config );
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance( config );

        int expectedResult = 0;
        IList<Integer> m1 = h1.getList( LIST_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.add( i );
            expectedResult += i;
        }

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( h1 );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        Map<String, Integer> result =
            task.mapper( new TestMapper() ).reducer( new TestDistributableReducer() ).submit();

        assertEquals( 1, result.size() );
        for ( Integer value : result.values() )
        {
            assertEquals( expectedResult, (int) value );
        }

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals( 0, hazelcastNames.size() );
    }

    @Test( timeout = 30000 )
    public void testClientListMapper()
        throws Exception
    {
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext( context );

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance( config );
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance( config );
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance( config );

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IList<Integer> m1 = client.getList( LIST_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.add( i );
        }

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        Map<String, List<Integer>> result = task.mapper( new TestMapper() ).submit();
        assertEquals( 1, result.size() );
        for ( List<Integer> value : result.values() )
        {
            assertEquals( 100, value.size() );
        }

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals( 0, hazelcastNames.size() );
    }

    @Test( timeout = 30000 )
    public void testAsyncListMapper()
        throws Exception
    {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory( 4 );
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext( context );

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance( config );
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance( config );
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance( config );

        IList<Integer> m1 = h1.getList( LIST_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.add( i );
        }

        final Map<String, List<Integer>> listenerResults = new HashMap<String, List<Integer>>();
        final Semaphore semaphore = new Semaphore( 1 );
        semaphore.acquire();

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( h1 );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        task.mapper( new TestMapper() ).submitAsync( new MapReduceListener<String, List<Integer>>()
        {

            @Override
            public void onCompletion( Map<String, List<Integer>> reducedResults )
            {
                listenerResults.putAll( reducedResults );
                semaphore.release();
            }
        } );

        semaphore.acquire();

        assertEquals( 1, listenerResults.size() );
        for ( List<Integer> value : listenerResults.values() )
        {
            assertEquals( 100, value.size() );
        }

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals( 0, hazelcastNames.size() );
    }

    @Test( timeout = 30000 )
    public void testAsyncClientListMapper()
        throws Exception
    {
        final CountingManagedContext context = new CountingManagedContext();
        final Config config = new Config();
        config.setManagedContext( context );

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance( config );
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance( config );
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance( config );

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IList<Integer> m1 = client.getList( LIST_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.add( i );
        }

        final Map<String, List<Integer>> listenerResults = new HashMap<String, List<Integer>>();
        final Semaphore semaphore = new Semaphore( 1 );
        semaphore.acquire();

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        task.mapper( new TestMapper() ).submitAsync( new MapReduceListener<String, List<Integer>>()
        {

            @Override
            public void onCompletion( Map<String, List<Integer>> reducedResults )
            {
                listenerResults.putAll( reducedResults );
                semaphore.release();
            }
        } );

        semaphore.acquire();

        assertEquals( 1, listenerResults.size() );
        for ( List<Integer> value : listenerResults.values() )
        {
            assertEquals( 100, value.size() );
        }

        Set<String> hazelcastNames = context.getHazelcastNames();
        assertEquals( 0, hazelcastNames.size() );
    }

    @SuppressWarnings( "serial" )
    public static class TestMapper
        extends Mapper<Integer, Integer, String, Integer>
    {

        @Override
        public void map( Integer key, Integer value, Collector<String, Integer> collector )
        {
            collector.emit( String.valueOf( key ), value );
        }
    }

    @SuppressWarnings( "serial" )
    public static class TestReducer
        implements Reducer<String, Integer>
    {

        @Override
        public Integer reduce( String key, Iterator<Integer> values )
        {
            int sum = 0;
            while ( values.hasNext() )
            {
                sum += values.next();
            }
            return sum;
        }
    }

    @Distributable
    @SuppressWarnings( "serial" )
    public static class TestDistributableReducer
        implements Reducer<String, Integer>
    {

        @Override
        public Integer reduce( String key, Iterator<Integer> values )
        {
            int sum = 0;
            while ( values.hasNext() )
            {
                sum += values.next();
            }
            return sum;
        }
    }

}
