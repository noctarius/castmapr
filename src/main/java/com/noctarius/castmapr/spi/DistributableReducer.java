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
 * The {@link DistributableReducer} interface can be used to mark a {@link Reducer} as distributable.<br>
 * Distributed {@link Reducer}s normally run multiple times on different hosts which for example is not a problem for
 * sum-functions but can be a problem for other algorithms.<br>
 * <b>Caution: {@link Reducer}s need to be marked as distributable in an explicit way!</b><br>
 * An alternative to the usage of this interface is annotating the {@link Reducer} implementation using the
 * {@link Distributable} annotation.
 * 
 * @author noctarius
 */
public interface DistributableReducer<Key, Value>
    extends Reducer<Key, Value>
{
}
