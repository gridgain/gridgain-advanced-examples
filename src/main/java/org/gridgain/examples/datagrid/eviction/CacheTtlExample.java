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

import org.apache.ignite.*;
import org.gridgain.examples.*;

import javax.cache.expiry.*;
import java.util.concurrent.*;

/**
 * Demonstrates how to use cache TTL.
 * <p>
 * Remote nodes should always be started with special configuration file:
 * {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}
 * or {@link ExampleNodeStartup} can be used.
 */
public class CacheTtlExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheTtlExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache TTL example started.");

            try (IgniteCache<Long, Long> cache = ignite.createCache(CACHE_NAME)) {
                // Expiry policy.
                ExpiryPolicy plc = new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1));

                // Put value with expiration.
                cache.withExpiryPolicy(plc).put(0L, 0L);

                Long val = cache.get(0L);

                if (val == null || val != 0L) throw new Exception("Failed to get proper value from cache: " + val);

                System.out.println("Waiting for entry to expire...");

                // Let entry expire.
                Thread.sleep(2000);

                // Get value from cache.
                val = cache.get(0L);

                if (val != null) throw new Exception("Entry should have been evicted due to eager TTL eviction: " + val);

                System.out.println(">>> Cache TTL example finished.");
            }
        }
    }
}
