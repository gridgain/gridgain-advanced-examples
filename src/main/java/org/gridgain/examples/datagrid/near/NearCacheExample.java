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

package org.gridgain.examples.datagrid.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.resources.*;

import java.util.concurrent.*;

/**
 *
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
                System.err.println("Start remote nodes to run this example.");
            }
            else {
                int key = -1;

                for (int i = 0; i < 1000; i++) {
                    GridCacheEntry<Object, Object> entry = g.cache(CACHE_NAME).entry(i);

                    if (!entry.backup() && !entry.primary()) {
                        key = i;

                        break;
                    }
                }

                if (key == -1)
                    throw new Exception("Failed to map key to remote node.");

                if (g.cache(CACHE_NAME).get(key) != null)
                    throw new Exception("Key should not be in cache: " + key);

                System.out.println("Key belonging to remote node: " + key);

                final int key0 = key;

                g.forRemotes().compute().call(new Callable<Object>() {
                    @GridInstanceResource
                    private Grid g0;

                    @Override public Object call() throws Exception {
                        System.out.println("Putting example key on node: " + g.localNode().id());

                        return g0.cache(CACHE_NAME).putx(key0, 10);
                    }
                }).get();

                if (g.cache(CACHE_NAME).peek(key) != null)
                    throw new Exception("Key should not be in cache: " + key);

                // This will create near entry.
                if (g.cache(CACHE_NAME).get(key) != 10)
                    throw new Exception("Unexpected value in cache: " + key);

                if (g.cache(CACHE_NAME).peek(key) != 10)
                    throw new Exception("Unexpected value in cache: " + key);

                g.forRemotes().compute().call(new Callable<Object>() {
                    @GridInstanceResource
                    private Grid g0;

                    @Override public Object call() throws Exception {
                        System.out.println("Updating example key on node: " + g.localNode().id());

                        return g0.cache(CACHE_NAME).putx(key0, 15);
                    }
                }).get();

                Object val = g.cache(CACHE_NAME).peek(key);

                if (val != 15)
                    throw new Exception("Unexpected value in cache: " + key + " " + val);

                g.forRemotes().compute().call(new Callable<Object>() {
                    @GridInstanceResource
                    private Grid g0;

                    @Override public Object call() throws Exception {
                        System.out.println("Removing example key on node: " + g.localNode().id());

                        return g0.cache(CACHE_NAME).remove(key0);
                    }
                }).get();

                if (g.cache(CACHE_NAME).peek(key) != null)
                    throw new Exception("Key should not be in cache: " + key);

                // This will create near entry.
                if (g.cache(CACHE_NAME).get(key) != null)
                    throw new Exception("Unexpected value in cache: " + key);
            }
        }
    }
}
