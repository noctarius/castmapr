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

package com.noctarius.castmapr;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SerialTest;
import com.noctarius.castmapr.spi.Collator;
import com.noctarius.castmapr.spi.Collector;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

@RunWith( HazelcastJUnit4ClassRunner.class )
@Category( SerialTest.class )
public class ClientMapReduceTest
    extends HazelcastTestSupport
{

    private static final String MAP_NAME = "default";

    @Before
    public void gc()
    {
        Runtime.getRuntime().gc();
    }

    @After
    public void cleanup()
    {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test( timeout = 20000 )
    public void testMapper()
        throws Exception
    {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IMap<Integer, Integer> m1 = client.getMap( MAP_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.put( i, i );
        }

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        Map<String, List<Integer>> result = task.mapper( new TestMapper() ).submit();
        assertEquals( 100, result.size() );
        for ( List<Integer> value : result.values() )
        {
            assertEquals( 1, value.size() );
        }
    }

    @Test( timeout = 20000 )
    public void testMapperReducer()
        throws Exception
    {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IMap<Integer, Integer> m1 = client.getMap( MAP_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.put( i, i );
        }

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        Map<String, Integer> result = task.mapper( new GroupingTestMapper() ).reducer( new TestReducer() ).submit();

        // Precalculate results
        int[] expectedResults = new int[4];
        for ( int i = 0; i < 100; i++ )
        {
            int index = i % 4;
            expectedResults[index] += i;
        }

        for ( int i = 0; i < 4; i++ )
        {
            assertEquals( expectedResults[i], (int) result.get( String.valueOf( i ) ) );
        }
    }

    @Test( timeout = 20000 )
    public void testMapperCollator()
        throws Exception
    {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IMap<Integer, Integer> m1 = client.getMap( MAP_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.put( i, i );
        }

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        int result = task.mapper( new GroupingTestMapper() ).submit( new GroupingTestCollator() );

        // Precalculate result
        int expectedResult = 0;
        for ( int i = 0; i < 100; i++ )
        {
            expectedResult += i;
        }

        for ( int i = 0; i < 4; i++ )
        {
            assertEquals( expectedResult, result );
        }
    }

    @Test( timeout = 20000 )
    public void testMapperReducerCollator()
        throws Exception
    {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IMap<Integer, Integer> m1 = client.getMap( MAP_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.put( i, i );
        }

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        int result = task.mapper( new GroupingTestMapper() ).reducer( new TestReducer() ).submit( new TestCollator() );

        // Precalculate result
        int expectedResult = 0;
        for ( int i = 0; i < 100; i++ )
        {
            expectedResult += i;
        }

        for ( int i = 0; i < 4; i++ )
        {
            assertEquals( expectedResult, result );
        }
    }

    @Test( timeout = 20000 )
    public void testAsyncMapper()
        throws Exception
    {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IMap<Integer, Integer> m1 = client.getMap( MAP_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.put( i, i );
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

        assertEquals( 100, listenerResults.size() );
        for ( List<Integer> value : listenerResults.values() )
        {
            assertEquals( 1, value.size() );
        }
    }

    @Test( timeout = 20000 )
    public void testAsyncMapperReducer()
        throws Exception
    {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IMap<Integer, Integer> m1 = client.getMap( MAP_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.put( i, i );
        }

        final Map<String, Integer> listenerResults = new HashMap<String, Integer>();
        final Semaphore semaphore = new Semaphore( 1 );
        semaphore.acquire();

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        task.mapper( new GroupingTestMapper() ).reducer( new TestReducer() ) //
        .submitAsync( new MapReduceListener<String, Integer>()
        {

            @Override
            public void onCompletion( Map<String, Integer> reducedResults )
            {
                listenerResults.putAll( reducedResults );
                semaphore.release();
            }
        } );

        // Precalculate results
        int[] expectedResults = new int[4];
        for ( int i = 0; i < 100; i++ )
        {
            int index = i % 4;
            expectedResults[index] += i;
        }

        semaphore.acquire();

        for ( int i = 0; i < 4; i++ )
        {
            assertEquals( expectedResults[i], (int) listenerResults.get( String.valueOf( i ) ) );
        }
    }

    @Test( timeout = 20000 )
    public void testAsyncMapperCollator()
        throws Exception
    {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IMap<Integer, Integer> m1 = client.getMap( MAP_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.put( i, i );
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore( 1 );
        semaphore.acquire();

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        task.mapper( new GroupingTestMapper() )//
        .submitAsync( new GroupingTestCollator(), new MapReduceCollatorListener<Integer>()
        {

            @Override
            public void onCompletion( Integer r )
            {
                result[0] = r.intValue();
                semaphore.release();
            }
        } );

        // Precalculate result
        int expectedResult = 0;
        for ( int i = 0; i < 100; i++ )
        {
            expectedResult += i;
        }

        semaphore.acquire();

        for ( int i = 0; i < 4; i++ )
        {
            assertEquals( expectedResult, result[0] );
        }
    }

    @Test( timeout = 20000 )
    public void testAsyncMapperReducerCollator()
        throws Exception
    {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient( null );
        IMap<Integer, Integer> m1 = client.getMap( MAP_NAME );
        for ( int i = 0; i < 100; i++ )
        {
            m1.put( i, i );
        }

        final int[] result = new int[1];
        final Semaphore semaphore = new Semaphore( 1 );
        semaphore.acquire();

        MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( client );
        MapReduceTask<Integer, Integer, String, Integer> task = factory.build( m1 );
        task.mapper( new GroupingTestMapper() ).reducer( new TestReducer() )//
        .submitAsync( new TestCollator(), new MapReduceCollatorListener<Integer>()
        {

            @Override
            public void onCompletion( Integer r )
            {
                result[0] = r.intValue();
                semaphore.release();
            }
        } );

        // Precalculate result
        int expectedResult = 0;
        for ( int i = 0; i < 100; i++ )
        {
            expectedResult += i;
        }

        semaphore.acquire();

        for ( int i = 0; i < 4; i++ )
        {
            assertEquals( expectedResult, result[0] );
        }
    }

    public static class TestMapper
        extends Mapper<Integer, Integer, String, Integer>
    {

        @Override
        public void map( Integer key, Integer value, Collector<String, Integer> collector )
        {
            collector.emit( String.valueOf( key ), value );
        }
    }

    public static class GroupingTestMapper
        extends Mapper<Integer, Integer, String, Integer>
    {

        @Override
        public void map( Integer key, Integer value, Collector<String, Integer> collector )
        {
            collector.emit( String.valueOf( key % 4 ), value );
        }
    }

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

    public static class GroupingTestCollator
        implements Collator<String, List<Integer>, Integer>
    {

        @Override
        public Integer collate( Map<String, List<Integer>> reducedResults )
        {
            int sum = 0;
            for ( List<Integer> values : reducedResults.values() )
            {
                for ( Integer value : values )
                {
                    sum += value;
                }
            }
            return sum;
        }

    }

    public static class TestCollator
        implements Collator<String, Integer, Integer>
    {

        @Override
        public Integer collate( Map<String, Integer> reducedResults )
        {
            int sum = 0;
            for ( Integer value : reducedResults.values() )
            {
                sum += value;
            }
            return sum;
        }

    }

}