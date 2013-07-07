package com.noctarius.castmapr.client;

public enum ClientMapReduceCollectionType
{
    IMap, IList, MultiMap;

    public static ClientMapReduceCollectionType byOrdinal( int ordinal )
    {
        for ( ClientMapReduceCollectionType type : ClientMapReduceCollectionType.values() )
        {
            if ( type.ordinal() == ordinal )
            {
                return type;
            }
        }
        throw new IllegalStateException( "Illegal ClientMapReduceCollectionType was found" );
    }
}
