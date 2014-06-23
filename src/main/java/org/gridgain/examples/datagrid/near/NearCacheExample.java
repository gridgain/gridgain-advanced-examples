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

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import java.util.concurrent.*;

/**
 * This example demonstrates near cache functionality.
 * <p>
 * This example requires at least 1 remote node to work.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}
 * or {@link CacheExampleNodeStartup} can be used.
 */
public class NearCacheExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache-near.xml")) {
            if (g.nodes().size() == 1) {
                System.err.println("Need to start at least 1 remote node to run this example.");
            }
            else {
                int key = -1;

                final GridCache<Integer, Integer> cache = g.cache(CACHE_NAME);

                // Clear caches before running example.
                cache.globalClearAll();

                // Find key for which this node is neither primary or backup.
                // This will guarantee that the found key will end up in the near cache,
                // and not in the main partitioned cache.
                for (int i = 0; i < 1000; i++) {
                    GridCacheEntry<Integer, Integer> entry = cache.entry(i);

                    assert entry != null;

                    if (!entry.backup() && !entry.primary()) {
                        key = i;

                        break;
                    }
                }

                if (key == -1)
                    throw new Exception("Failed to map key to remote node.");

                if (cache.get(key) != null)
                    throw new Exception("Key should not be in cache: " + key);

                System.out.println("Key belonging to remote node: " + key);

                final int key0 = key;

                // Update key on remote node to make sure that it won't
                // be present in the local near cache.
                g.forRemotes().compute().call(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        System.out.println("Putting sample key on node: " + g.localNode().id());

                        return cache.putx(key0, 10);
                    }
                }).get();

                if (cache.peek(key) != null)
                    throw new Exception("Key should not be in cache: " + key);

                // This will create near entry.
                if (cache.get(key) != 10)
                    throw new Exception("Unexpected value in cache: " + key);

                if (cache.peek(key) != 10)
                    throw new Exception("Unexpected value in cache: " + key);

                // Update key on remote node to make sure that it won't
                // be present in the local near cache.
                g.forRemotes().compute().call(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        System.out.println("Updating sample key on node: " + g.localNode().id());

                        return cache.putx(key0, 15);
                    }
                }).get();

                // Peek into near cache.
                Object val = cache.peek(key);

                if (val != 15)
                    throw new Exception("Unexpected value in cache [" + key + "=" + val + ']');

                g.forRemotes().compute().call(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        System.out.println("Removing sample key on node: " + g.localNode().id());

                        return cache.remove(key0);
                    }
                }).get();

                if (cache.peek(key) != null)
                    throw new Exception("Key should not be in cache: " + key);

                // This will create near entry.
                if (cache.get(key) != null)
                    throw new Exception("Unexpected value in cache: " + key);
            }
        }
    }
}
