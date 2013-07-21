package com.noctarius.castmapr.core.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

abstract class AbstractMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
    extends AbstractNamedOperation
    implements PartitionAwareOperation
{

    protected Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    protected Reducer<KeyOut, ValueOut> reducer;

    protected List<KeyIn> keys;

    protected transient Object response;

    AbstractMapReduceOperation()
    {
    }

    AbstractMapReduceOperation( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                Reducer<KeyOut, ValueOut> reducer, List<KeyIn> keys )
    {
        super( name );
        this.mapper = mapper;
        this.reducer = reducer;
        this.keys = keys;
    }

    @Override
    public boolean returnsResponse()
    {
        return true;
    }

    @Override
    public Object getResponse()
    {
        return response;
    }

    @Override
    protected void writeInternal( ObjectDataOutput out )
        throws IOException
    {
        super.writeInternal( out );
        out.writeObject( mapper );
        out.writeObject( reducer );
        out.writeInt( keys == null ? 0 : keys.size() );
        if ( keys != null )
        {
            for ( KeyIn key : keys )
            {
                out.writeObject( key );
            }
        }
    }

    @Override
    protected void readInternal( ObjectDataInput in )
        throws IOException
    {
        super.readInternal( in );
        mapper = in.readObject();
        reducer = in.readObject();
        int size = in.readInt();
        keys = new ArrayList<KeyIn>( size );
        for ( int i = 0; i < size; i++ )
        {
            keys.add( (KeyIn) in.readObject() );
        }
    }

}
