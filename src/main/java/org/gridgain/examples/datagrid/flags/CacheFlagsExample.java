/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.flags;

import org.gridgain.examples.datagrid.*;
import org.gridgain.examples.datagrid.model.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.cloner.*;

/**
 * This example shows how to use cache flags by the example of {@code SKIP_STORE} and {@code CLONE}
 * flags.
 */
public class CacheFlagsExample {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        GridConfiguration c = new GridConfiguration();

        c.setLocalHost("localhost");

        GridCacheConfiguration cc = new GridCacheConfiguration();

        CacheMapStore<Object, Object> store = new CacheMapStore<>();

        cc.setName("test");
        cc.setCacheMode(GridCacheMode.PARTITIONED);
        cc.setStore(store);
        cc.setCloner(new GridCacheDeepCloner());

        c.setCacheConfiguration(cc);

        try (Grid g = GridGain.start(c)) {
            GridCache<String, Organization> cache = g.cache("test");

            // Create projection with SKIP_STORE flag enabled.
            GridCacheProjection<String, Organization> skipStorePrj = cache.flagsOn(GridCacheFlag.SKIP_STORE);

            String key = "org";

            skipStorePrj.put(key, new Organization("TestOrganization"));

            // Check that store is not invoked when SKIP_STORE flag is set.
            System.out.println(">>> Store contents after putting value with SKIP_STORE flag: " + store.storeMap());

            cache.put(key, new Organization("TestOrganization"));

            System.out.println(">>> Store contents after putting value without SKIP_STORE flag: " + store.storeMap());

            // Create projection with CLONE flag enabled.
            GridCacheProjection<String, Organization> clonePrj = cache.flagsOn(GridCacheFlag.CLONE);

            Organization cloned1 = clonePrj.get(key);
            Organization cloned2 = clonePrj.get(key);

            Organization simple1 = cache.get(key);
            Organization simple2 = cache.get(key);

            // Check that cloned objects are not equal by reference.
            System.out.println("Equality test for CLONE flag [(cloned1 == cloned2)=" + (cloned1 == cloned2) +
                ", (cloned1.equals(cloned2) = " + (cloned1.equals(cloned2)) + ']');

            // Check that objects obtained without CLONE flag are the same.
            System.out.println("Equality test for CLONE flag [(simple1 == simple2)=" + (simple1 == simple2) +
                ", (simple1.equals(simple2) = " + (simple1.equals(simple2)) + ']');
        }
    }
}
