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

package org.gridgain.examples.datagrid.dataload;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.dataload.*;

/**
 * Demonstrates how cache can be populated with data utilizing {@link GridDataLoader} API.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}
 * or {@link CacheExampleNodeStartup} can be used.
 */
public class CacheDataLoaderExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 500000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache data loader example started.");

            // Clear caches before running example.
            g.cache(CACHE_NAME).globalClearAll();

            try (GridDataLoader<Integer, Integer> ldr = g.dataLoader(CACHE_NAME)) {
                // Configure loader.
                ldr.perNodeBufferSize(1024);

                long start = System.currentTimeMillis();

                for (int i = 0; i < ENTRY_COUNT; i++) {
                    ldr.addData(i, i);

                    // Print out progress while loading cache.
                    if (i > 0 && i % 10000 == 0)
                        System.out.println("Loaded " + i + " keys.");
                }

                long end = System.currentTimeMillis();

                System.out.println(">>> Loaded " + ENTRY_COUNT + " keys in " + (end - start) + "ms.");
            }

            System.out.println(">>> Hit enter to stop the node.");

            System.in.read();
        }
    }
}
