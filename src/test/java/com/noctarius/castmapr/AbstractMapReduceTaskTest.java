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

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractMapReduceTaskTest
    extends HazelcastTestSupport
{

    protected ClientConfig buildClientConfig()
    {
        ClientConfig config = new XmlClientConfigBuilder().build();
        config.setManagedContext( new CountingManagedContext() );
        return config;
    }

    protected Config buildConfig()
    {
        Config config = new XmlConfigBuilder().build();
        config.setManagedContext( new CountingManagedContext() );
        return config;
    }

    public static interface CountingAware
        extends HazelcastInstanceAware
    {
        void setCouter( Set<String> hazelcastNames );
    }

    public static class CountingManagedContext
        implements ManagedContext
    {

        private final Set<String> hazelcastNames = new HashSet<String>();

        @Override
        public Object initialize( Object obj )
        {
            if ( obj instanceof CountingAware )
            {
                ( (CountingAware) obj ).setCouter( hazelcastNames );
            }
            return obj;
        }

        public Set<String> getHazelcastNames()
        {
            return hazelcastNames;
        }
    }
}
