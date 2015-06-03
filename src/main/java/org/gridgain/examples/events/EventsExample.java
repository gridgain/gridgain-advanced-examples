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

package org.gridgain.examples.events;

import org.apache.ignite.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.examples.*;

import java.util.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}
 * or {@link ExampleNodeStartup} can be used.
 */
public class EventsExample {
    /** Cache name. */
    private static final String CACHE_NAME = EventsExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            try (IgniteCache<Integer, String> cache = ignite.createCache(CACHE_NAME)) {
                // Listen to events happening on local node.
                localListen();

                // Wait for a while while callback is notified about remaining puts.
                Thread.sleep(1000);

                // Listen to events happening on all grid nodes.
                remoteListen();

                // Wait for a while while callback is notified about remaining puts.
                Thread.sleep(1000);
            }
        }
    }

    /**
     * Listen to events that happen only on local node.
     */
    private static void localListen() throws Exception {
        System.out.println();
        System.out.println(">>> Local event listener example.");

        Ignite ignite = Ignition.ignite();

        IgnitePredicate<CacheEvent> lsnr = new IgnitePredicate<CacheEvent>() {
            @Override public boolean apply(CacheEvent evt) {
                System.out.println("Received cache event [evt=" + evt.name() + ", cacheName=" + evt.cacheName() +
                    ", key=" + evt.key() + ']');

                return true; // Return true to continue listening.
            }
        };

        // Register event listener for all local task execution events.
        ignite.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT);

        // Generate cache events.
        for (int i = 0; i < 10; i++)
            ignite.cache(CACHE_NAME).put(i, String.valueOf(i));

        // Unsubscribe local task event listener.
        ignite.events().stopLocalListen(lsnr);
    }

    /**
     * Listen to events coming from all grid nodes.
     */
    private static void remoteListen() {
        System.out.println();
        System.out.println(">>> Remote event listener example.");

        // This optional local callback is called for each event notification
        // that passed remote predicate listener.
        IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
            @Override public boolean apply(UUID nodeId, CacheEvent evt) {
                System.out.println("Received cache event [evt=" + evt.name() + ", cacheName=" + evt.cacheName() +
                    ", key=" + evt.key() + ']');

                return true; // Return true to continue listening.
            }
        };

        // Remote filter which only accepts tasks whose name begins with "good-task" prefix.
        IgnitePredicate<CacheEvent> rmtLsnr = new IgnitePredicate<CacheEvent>() {
            @Override public boolean apply(CacheEvent evt) {
                Integer key = evt.key();

                if (key != null && key % 2 == 0) {
                    System.out.println("Filter passed for key: " + key);

                    return true;
                }

                return false;
            }
        };

        Ignite ignite = Ignition.ignite();

        // Register event listeners on all nodes to listen for task events.
        UUID lsnrId = ignite.events().remoteListen(locLsnr, rmtLsnr, EVT_CACHE_OBJECT_PUT);

        // Generate cache events.
        for (int i = 0; i < 10; i++)
            ignite.cache(CACHE_NAME).put(i, String.valueOf(i));

        ignite.events().stopRemoteListen(lsnrId);
    }
}
