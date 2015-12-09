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

package org.gridgain.examples.clustering;

import java.util.Random;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.Ignition;
import org.gridgain.examples.ExampleNodeStartup;

/**
 * Example demonstrates client automatic reconnection feature.
 *
 * Follow the steps below to see the example in action:
 * - Start {@link ExampleNodeStartup};
 * - Start this example;
 * - Stop a remote node started with {@link ExampleNodeStartup};
 * - Wait for client disconnection messsage;
 * - Start {@link ExampleNodeStartup} once again and see that the client will reconnect automatically to the cluster.
 */
public class ClientReconnectExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            if (ignite.cluster().nodes().size() < 2)
                throw new RuntimeException("Not enough nodes in the topology to demonstrate the example");

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("test_cache");

            Random rand = new Random();

            while (true) {
                for (int i = 0; i < 1000; i++) {
                    try {
                        System.out.println("Put: " + i);

                        cache.getAndPut(i, rand.nextInt(10_000));

                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                            return;
                        }
                    }
                    catch (CacheException e) {
                        if (e.getCause() instanceof IgniteClientDisconnectedException) {
                            IgniteClientDisconnectedException ex = (IgniteClientDisconnectedException)e.getCause();

                            System.out.println("Client lost connection to the cluster. Waiting for reconnect...");

                            // Waiting until the client is reconnected.
                            ex.reconnectFuture().get();

                            System.out.println("Client has been reconnected to the cluster.");

                            // Can safely proceed with the same cache instance only when the cache really exists.
                            try {
                                cache.get(0);
                            }
                            catch (IllegalStateException e2) {
                                cache = ignite.getOrCreateCache("test_cache");
                            }
                        }
                        else {
                            e.printStackTrace();
                            return;
                        }
                    }
                }
            }
        }
    }
}
