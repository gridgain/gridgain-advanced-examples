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

package org.gridgain.examples.datagrid.eviction;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

/**
 * Demonstrates how to use cache TTL.
 * <p>
 * Remote nodes should always be started with special configuration file:
 * {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}
 * or {@link CacheExampleNodeStartup} can be used.
 */
public class CacheTtlExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache TTL example started.");

            final GridCache<Long, Long> cache = g.cache(CACHE_NAME);

            // Get empty cache entry.
            GridCacheEntry<Long, Long> e = cache.entry(0L);

            assert e != null;

            // Set time to live first.
            e.timeToLive(1000);

            // Put value to cache.
            e.set(0L);

            Long val = cache.get(0L);

            if (val == null || val != 0L)
                throw new Exception("Failed to get proper value from cache: " + val);

            System.out.println("Waiting for entry to expire...");

            // Let entry expire.
            Thread.sleep(2000);

            // Get value via GridCacheEntry.
            val = e.get();

            if (val != null)
                throw new Exception("Entry should have been evicted due to eager TTL eviction: " + val);

            // Get value from cache.
            val = cache.get(0L);

            if (val != null)
                throw new Exception("Entry should have been evicted due to eager TTL eviction: " + val);

            System.out.println(">>> Cache TTL example finished.");
        }
    }
}
