package com.noctarius.castmapr.core.operation;

import java.util.List;

import com.hazelcast.spi.Operation;

public interface MapReduceOperationFactory<Key>
{

    Operation createOperation( int partitionId, List<Key> keys );

}
