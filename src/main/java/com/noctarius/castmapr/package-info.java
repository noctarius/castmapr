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

/**
 * <p>This package contains the basic key classes / interfaces for usage of CastMapR with Hazelcast 3.0.</p>
 * <p>A complete example of the usage can look like this:
 * <pre>
 * public class CastMapRDemo
 * {
 *   public static void main(String[] args)
 *   {
 *     HazelcastInstance hazelcast = nodeFactory.newHazelcastInstance();
 *     IMap<Integer, Integer> map = hazelcast.getMap( "PlayerLogins" );
 *     
 *     MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( hazelcast );
 *     
 *     MapReduceTask<Integer, Long, Integer, Integer> task = factory.build( map );
 *     
 *     Map<Integer, Integer> loginCounts = task.mapper( new PlayerMapper() )
 *          .reducer( new LoginReducer() ).submit();
 *          
 *     for ( Entry<Integer, Integer> entry : loginCounts.entrySet() )
 *     {
 *       System.out.println( "Player " + entry.getKey() + " has " +
 *              entry.getValue() + " logins." );
 *     } 
 *   }
 * }
 * 
 * public class PlayerMapper extends Mapper<Integer, Long, Integer, Integer>
 * {
 *   public void map( Integer playerId, Long timestamp, Collector<Integer, Integer> collector )
 *   {
 *     // We are interested in the count of player logins so we discard the timestamp information
 *     collector.emit( playerId, 1 );
 *   }
 * }
 * 
 * public class LoginReducer implements DistributableReducer<Integer, Integer>
 * {
 *   public void reduce( Integer playerId, Iterator<Integer> values )
 *   {
 *     int count = 0;
 *     while ( values.hasNext() )
 *     {
 *       values.next();
 *       count++;
 *     }
 *     return count;
 *   }
 * }
 * </pre>
 * </p>
 */
package com.noctarius.castmapr;


