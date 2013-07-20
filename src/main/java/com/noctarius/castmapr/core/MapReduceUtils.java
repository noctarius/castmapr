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
