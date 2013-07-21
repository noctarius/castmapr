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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hazelcast.partition.PartitionService;

public final class MapReduceUtils
{

    private MapReduceUtils()
    {
    }

    public static <Key> Iterable<Key> copyKeys( Iterable<Key> keys )
    {
        if ( keys == null )
        {
            return null;
        }
        List<Key> result = new ArrayList<Key>();
        for ( Key key : keys )
        {
            result.add( key );
        }
        // Force correct size recreation
        return new ArrayList<Key>( result );
    }

    public static <Key> Map<Integer, List<Key>> mapKeysToPartitions( PartitionService partitionService, List<Key> keys )
    {
        Map<Integer, List<Key>> keyMapping = new HashMap<Integer, List<Key>>();
        for ( Key key : keys )
        {
            int partitionId = partitionService.getPartitionId( key );
            List<Key> mappedKeys = keyMapping.get( partitionId );
            if ( mappedKeys == null )
            {
                mappedKeys = new ArrayList<Key>();
                keyMapping.put( partitionId, mappedKeys );
            }
            mappedKeys.add( key );
        }
        return keyMapping;
    }

}
