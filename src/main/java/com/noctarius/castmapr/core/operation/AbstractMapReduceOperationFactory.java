package com.noctarius.castmapr.core.operation;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.OperationFactory;
import com.noctarius.castmapr.spi.Mapper;
import com.noctarius.castmapr.spi.Reducer;

public abstract class AbstractMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>
    implements OperationFactory
{

    protected Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    protected Reducer<KeyOut, ValueOut> reducer;

    protected boolean distributableReducer;

    protected String name;

    AbstractMapReduceOperationFactory()
    {
    }

    AbstractMapReduceOperationFactory( String name, Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                       Reducer<KeyOut, ValueOut> reducer, boolean distributableReducer )
    {
        this.name = name;
        this.mapper = mapper;
        this.reducer = reducer;
        this.distributableReducer = distributableReducer;
    }

    @Override
    public void writeData( ObjectDataOutput out )
        throws IOException
    {
        out.writeUTF( name );
        out.writeObject( mapper );
        out.writeObject( reducer );
        out.writeBoolean( distributableReducer );
    }

    @Override
    public void readData( ObjectDataInput in )
        throws IOException
    {
        name = in.readUTF();
        mapper = in.readObject();
        reducer = in.readObject();
        distributableReducer = in.readBoolean();
    }

}
