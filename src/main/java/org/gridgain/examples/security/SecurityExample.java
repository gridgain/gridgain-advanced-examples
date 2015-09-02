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

package org.gridgain.examples.security;

import org.apache.ignite.*;
import org.gridgain.examples.*;

/**
 * This example demonstrates authentication and authorization support in GridGain.
 * Start {@link ExampleNodeStartup} with config/security/security-data-node.xml or
 * {@code 'ggstart.[sh|bat] ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}.
 * Then start the example. It will start 3 nodes sequentially:
 * <ol>
 *     <li>The first one started in {@link #startBadNode()} will not get into topology and start
 *     will fail with exception.</li>
 *     <li>The second one started in {@link #startReadWriteNode()} will successfully start and
 *     execute cache read and write operations which are permitted
 *     because of the credentials it is configured with.</li>
 *     <li>The third one started in {@link #startReadOnlyNode()} will successfully start and
 *     execute cache read but will fail on cache write operation which is fine
 *     because of the credentials it is configured with.</li>
 * </ol>
 */
public class SecurityExample {
    /**
     * @param args Arguments (none required).
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Security example started.");

        startBadNode();

        startReadWriteNode();

        startReadOnlyNode();
    }

    /**
     *
     */
    private static void startBadNode() {
        try {
            Ignition.start("config/security/security-client-node-bad.xml");

            throw new Error(); // Should never happen.
        }
        catch (Exception e) {
            System.out.println();
            System.out.println();
            System.out.println("Caught expected exception: " + e);
        }
    }

    /**
     *
     */
    private static void startReadOnlyNode() {
        try (Ignite ignite = Ignition.start("config/security/security-client-node-readonly.xml")) {
            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("partitioned");

            System.out.println("Read from cache: " + cache.get(1));

            try {
                cache.put(2, 2);

                throw new Error(); // Should never happen.
            }
            catch (Exception e) {
                System.out.println();
                System.out.println();
                System.out.println("Caught expected exception: " + e);
            }
        }
    }

    /**
     *
     */
    private static void startReadWriteNode() {
        try (Ignite ignite = Ignition.start("config/security/security-client-node-readwrite.xml")) {
            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("partitioned");

            cache.put(1, 1);
            cache.put(2, 2);

            System.out.println("Read from cache: " + cache.get(1));
        }
    }
}
