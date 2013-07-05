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

import java.lang.reflect.Constructor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.util.ExceptionUtil;

public class MapReduceTaskFactory
{

    private static final String NODE_HI_CLASS = "com.hazelcast.instance.HazelcastInstanceProxy";

    private static final String CLIENT_HI_CLASS = "com.hazelcast.client.HazelcastClientProxy";

    private final ClassLoader classLoader = MapReduceTaskFactory.class.getClassLoader();

    private final MapReduceTaskBuilder mapReduceTaskBuilder;

    private final HazelcastInstance hazelcastInstance;

    public static MapReduceTaskFactory newInstance( HazelcastInstance hazelcastInstance )
    {
        return new MapReduceTaskFactory( hazelcastInstance );
    }

    private MapReduceTaskFactory( HazelcastInstance hazelcastInstance )
    {
        this.hazelcastInstance = hazelcastInstance;
        this.mapReduceTaskBuilder = buildMapReduceTaskBuilder( hazelcastInstance );
    }

    private MapReduceTaskBuilder buildMapReduceTaskBuilder( HazelcastInstance hazelcastInstance )
    {
        try
        {
            String canonicalClassName = hazelcastInstance.getClass().getCanonicalName();

            String className;
            if ( NODE_HI_CLASS.equals( canonicalClassName ) )
            {
                className = "com.noctarius.castmapr.NodeMapReduceTaskBuilder";
            }
            else if ( CLIENT_HI_CLASS.equals( canonicalClassName ) )
            {
                className = "com.noctarius.castmapr.ClientMapReduceTaskBuilder";
            }
            else
            {
                throw new IllegalArgumentException( "Unknown HazelcastInstance implementation found." );
            }

            Class<? extends MapReduceTaskBuilder> builderClass;
            builderClass = (Class<? extends MapReduceTaskBuilder>) classLoader.loadClass( className );

            Constructor<MapReduceTaskBuilder> constructor;
            constructor =
                (Constructor<MapReduceTaskBuilder>) builderClass.getDeclaredConstructor( HazelcastInstance.class );

            return constructor.newInstance( hazelcastInstance );
        }
        catch ( Throwable t )
        {
            ExceptionUtil.rethrow( t );
        }
        return null;
    }

    public <KeyIn, ValueIn, KeyOut, ValueOut> MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut> build( IMap<KeyIn, ValueIn> map )
    {
        return mapReduceTaskBuilder.build( map );
    }

}
