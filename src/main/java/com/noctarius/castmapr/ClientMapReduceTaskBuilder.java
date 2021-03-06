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

import java.lang.reflect.Method;

import com.hazelcast.client.proxy.ClientListProxy;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.proxy.ClientMultiMapProxy;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.castmapr.client.IListClientMapReduceTaskProxy;
import com.noctarius.castmapr.client.IMapClientMapReduceTaskProxy;
import com.noctarius.castmapr.client.MultiMapClientMapReduceTaskProxy;

class ClientMapReduceTaskBuilder<KeyIn, ValueIn, KeyOut, ValueOut>
    implements MapReduceTaskBuilder<KeyIn, ValueIn, KeyOut, ValueOut>
{

    private static final Method GET_CLIENTCONTEXT_METHOD;

    static
    {
        Method getClientContextMethod = null;
        try
        {
            getClientContextMethod = ClientProxy.class.getDeclaredMethod( "getContext" );
            getClientContextMethod.setAccessible( true );
        }
        catch ( Throwable t )
        {
            ExceptionUtil.rethrow( t );
        }
        GET_CLIENTCONTEXT_METHOD = getClientContextMethod;
    }

    private final HazelcastInstance hazelcastInstance;

    ClientMapReduceTaskBuilder( HazelcastInstance hazelcastInstance )
    {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut> build( IMap<KeyIn, ValueIn> map )
    {
        try
        {
            ClientMapProxy<KeyIn, ValueIn> proxy = (ClientMapProxy<KeyIn, ValueIn>) map;
            ClientContext context = (ClientContext) GET_CLIENTCONTEXT_METHOD.invoke( proxy );
            return new IMapClientMapReduceTaskProxy<KeyIn, ValueIn, KeyOut, ValueOut>( proxy.getName(), context,
                                                                                       hazelcastInstance );
        }
        catch ( Throwable t )
        {
            ExceptionUtil.rethrow( t );
        }
        return null;
    }

    @Override
    public MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut> build( MultiMap<KeyIn, ValueIn> multiMap )
    {
        try
        {
            ClientMultiMapProxy<KeyIn, ValueIn> proxy = (ClientMultiMapProxy<KeyIn, ValueIn>) multiMap;
            ClientContext context = (ClientContext) GET_CLIENTCONTEXT_METHOD.invoke( proxy );
            return new MultiMapClientMapReduceTaskProxy<KeyIn, ValueIn, KeyOut, ValueOut>( proxy.getName(), context,
                                                                                           hazelcastInstance );
        }
        catch ( Throwable t )
        {
            ExceptionUtil.rethrow( t );
        }
        return null;
    }

    @Override
    public MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut> build( IList<ValueIn> list )
    {
        try
        {
            ClientListProxy<ValueIn> proxy = (ClientListProxy<ValueIn>) list;
            ClientContext context = (ClientContext) GET_CLIENTCONTEXT_METHOD.invoke( proxy );
            return new IListClientMapReduceTaskProxy<KeyIn, ValueIn, KeyOut, ValueOut>( proxy.getName(), context,
                                                                                        hazelcastInstance );
        }
        catch ( Throwable t )
        {
            ExceptionUtil.rethrow( t );
        }
        return null;
    }
}
