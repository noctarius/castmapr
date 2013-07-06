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

package com.noctarius.castmapr.spi;

/**
 * This interface can be used to mark {@link Mapper} or {@link Reducer} implementation being aware of the data partition
 * it is currently working on.
 * 
 * @author noctarius
 */
public interface PartitionIdAware
{

    /**
     * This method is called after deserializing but before executing {@link Mapper#map(Object, Object, Collector)} or
     * {@link Reducer#reduce(Object, java.util.Iterator)}.
     * 
     * @param partitionId The current partitionId
     */
    void setPartitionId( int partitionId );

}
