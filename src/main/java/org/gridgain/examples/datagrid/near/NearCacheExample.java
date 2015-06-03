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

package org.gridgain.examples.datagrid.near;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.examples.*;

/**
 * This example demonstrates near cache functionality.
 * <p>
 * This example requires at least 1 remote node to work.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}
 * or {@link ExampleNodeStartup} can be used.
 */
public class NearCacheExample {
    /** */
    private static final String CACHE_NAME = NearCacheExample.class.getName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        // Start node in client mode.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            if (ignite.cluster().nodes().size() == 1) {
                System.err.println("Need to start at least 1 remote node to run this example.");
            }
            else {
                // Create cache with all defaults and near cache on local node.
                CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(CACHE_NAME);
                NearCacheConfiguration<Integer, Integer> nearCfg = new NearCacheConfiguration<>();

                try (IgniteCache<Integer, Integer> cache = ignite.createCache(cfg, nearCfg)) {
                    final int key = 1;

                    if (cache.get(key) != null)
                        throw new Exception("Key should not be in cache: " + key);

                    ClusterGroup remotes = ignite.cluster().forRemotes();

                    // Update key on remote node to make sure that it won't
                    // be present in the local near cache.
                    ignite.compute(remotes).run(new IgniteRunnable() {
                        @Override public void run() {
                            System.out.println("Putting sample key on node: " + ignite.cluster().localNode().id());

                            cache.put(key, 10);
                        }
                    });

                    if (cache.localPeek(key) != null)
                        throw new Exception("Key should not be in near cache: " + key);

                    // This will create near entry.
                    if (cache.get(key) != 10)
                        throw new Exception("Unexpected value in cache: " + key);

                    if (cache.localPeek(key) != 10)
                        throw new Exception("Unexpected value in near cache: " + key);

                    // Update key on remote node.
                    ignite.compute(remotes).run(new IgniteRunnable() {
                        @Override public void run() {
                            System.out.println("Putting sample key on node: " + ignite.cluster().localNode().id());

                            cache.put(key, 15);
                        }
                    });

                    // Peek into near cache.
                    int val = cache.localPeek(key);

                    if (val != 15)
                        throw new Exception("Unexpected value in cache [" + key + "=" + val + ']');

                    // Remove key on remote node.
                    ignite.compute(remotes).run(new IgniteRunnable() {
                        @Override public void run() {
                            System.out.println("Putting sample key on node: " + ignite.cluster().localNode().id());

                            cache.remove(key);
                        }
                    });

                    if (cache.localPeek(key) != null)
                        throw new Exception("Key should not be in near cache: " + key);

                    if (cache.get(key) != null)
                        throw new Exception("Key should not be in cache: " + key);
                }
            }
        }
    }
}
