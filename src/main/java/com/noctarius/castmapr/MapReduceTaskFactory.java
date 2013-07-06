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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.util.ExceptionUtil;

/**
 * <p>
 * The MapReduceTaskFactory is used to create instances of {@link MapReduceTask}s depending on the
 * {@link HazelcastInstance} that is given to the {@link #newInstance(HazelcastInstance)} method.
 * </p>
 * <p>
 * The underlying created instance of the {@link MapReduceTask} depends on whether it is used for a
 * {@link HazelcastClient} or a {@link Hazelcast} member node.
 * </p>
 * <p>
 * The default usage is same for both cases and looks similar to the following example:<br>
 * 
 * <pre>
 * HazelcastInstance hazelcastInstance = getHazelcastInstance();
 * IMap map = hazelcastInstance.getMap( getMapName() );
 * MapReduceTaskFactory factory = MapReduceTaskFactory.newInstance( hazelcastInstance );
 * MapReduceTask task = factory.build( map );
 * </pre>
 * </p>
 * <p>
 * The created instance of MapReduceTaskFactory is fully threadsafe and can be used concurrently and multiple times.<br>
 * <b>Caution: Do not use the MapReduceTaskFactory with other instances of {@link HazelcastInstance} than the given one
 * for creation of the factory. Unexpected results could happen!</b>
 * </p>
 * 
 * @author noctarius
 */
public class MapReduceTaskFactory
{

    private static final String NODE_HI_CLASS = "com.hazelcast.instance.HazelcastInstanceProxy";

    private static final String CLIENT_HI_CLASS = "com.hazelcast.client.HazelcastClientProxy";

    private final ClassLoader classLoader = MapReduceTaskFactory.class.getClassLoader();

    private final MapReduceTaskBuilder mapReduceTaskBuilder;

    private final HazelcastInstance hazelcastInstance;

    /**
     * Creates a new instance of the MapReduceTaskFactory depending on the given {@link HazelcastInstance}.
     * 
     * @param hazelcastInstance The {@link HazelcastInstance} this MapReduceTaskFactory is used for.
     * @return A MapReduceTaskFactory instance that is bound to the given {@link HazelcastInstance}.
     */
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

    /**
     * Builds a {@link MapReduceTask} instance for the given {@link IMap} instance. The returning implementation is
     * depending on the {@link HazelcastInstance} given while creating the MapReduceTaskFactory.<br>
     * <b>Caution: Do not use the MapReduceTaskFactory with other instances of {@link HazelcastInstance} than the given
     * one for creation of the factory. Unexpected results could happen!</b>
     * 
     * @param map The map the created {@link MapReduceTask} should work on.
     * @return An instance of the {@link MapReduceTask} bound to the given {@link IMap}.
     */
    public <KeyIn, ValueIn, KeyOut, ValueOut> MapReduceTask<KeyIn, ValueIn, KeyOut, ValueOut> build( IMap<KeyIn, ValueIn> map )
    {
        return mapReduceTaskBuilder.build( map );
    }

}
