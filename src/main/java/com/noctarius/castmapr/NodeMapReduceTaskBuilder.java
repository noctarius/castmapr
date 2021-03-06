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

import com.hazelcast.collection.list.ObjectListProxy;
import com.hazelcast.collection.multimap.MultiMapProxySupport;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.map.proxy.MapProxyImpl;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import com.noctarius.castmapr.core.IListNodeMapReduceTaskImpl;
import com.noctarius.castmapr.core.IMapNodeMapReduceTaskImpl;
import com.noctarius.castmapr.core.MultiMapNodeMapReduceTaskImpl;

class NodeMapReduceTaskBuilder<KeyIn, ValueIn, KeyOut, ValueOut>
    implements MapReduceTaskBuilder<KeyIn, ValueIn, KeyOut, ValueOut>
{

    private static final Method GET_ORIGINAL_METHOD;

    static
    {
        Method getOriginalMethod = null;
        try
        {
            getOriginalMethod = HazelcastInstanceProxy.class.getDeclaredMethod( "getOriginal" );
            getOriginalMethod.setAccessible( true );
        }
        catch ( Throwable t )
        {
            ExceptionUtil.rethrow( t );
        }
        GET_ORIGINAL_METHOD = getOriginalMethod;
    }

    private final HazelcastInstanceImpl hazelcastInstance;

    NodeMapReduceTaskBuilder( HazelcastInstance hazelcastInstance )
    {
        HazelcastInstanceImpl instance = null;
        try
        {
            HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) hazelcastInstance;
            instance = (HazelcastInstanceImpl) GET_ORIGINAL_METHOD.invoke( proxy );
        }
        catch ( Throwable t )
        {
            ExceptionUtil.rethrow( t );
        }
        this.hazelcastInstance = instance;
    }

    @Override
    public MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut> build( IMap<KeyIn, ValueIn> map )
    {
        try
        {
            MapProxyImpl<KeyIn, ValueIn> proxy = (MapProxyImpl<KeyIn, ValueIn>) map;
            NodeEngine nodeEngine = hazelcastInstance.node.nodeEngine;
            return new IMapNodeMapReduceTaskImpl<KeyIn, ValueIn, KeyOut, ValueOut>( proxy.getName(), nodeEngine,
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
            MultiMapProxySupport proxy = (MultiMapProxySupport) multiMap;
            NodeEngine nodeEngine = hazelcastInstance.node.nodeEngine;
            return new MultiMapNodeMapReduceTaskImpl<KeyIn, ValueIn, KeyOut, ValueOut>( proxy.getName(), nodeEngine,
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
        ObjectListProxy<ValueIn> proxy = (ObjectListProxy<ValueIn>) list;
        NodeEngine nodeEngine = hazelcastInstance.node.nodeEngine;
        return new IListNodeMapReduceTaskImpl<KeyIn, ValueIn, KeyOut, ValueOut>( proxy.getName(), nodeEngine,
                                                                                 hazelcastInstance );
    }

}
