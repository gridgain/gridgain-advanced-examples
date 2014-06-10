/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.events;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 */
public class EventsExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid grid = GridGain.start("config/example-cache.xml")) {
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

    /**
     * Listen to events that happen only on local node.
     *
     * @throws GridException If failed.
     */
    private static void localListen() throws Exception {
        System.out.println();
        System.out.println(">>> Local event listener example.");

        Grid g = GridGain.grid();

        GridPredicate<GridCacheEvent> lsnr = new GridPredicate<GridCacheEvent>() {
            @Override public boolean apply(GridCacheEvent evt) {
                System.out.println("Received cache event [evt=" + evt.name() + ", cacheName=" + evt.cacheName() +
                    ", key=" + evt.key() + ']');

                return true; // Return true to continue listening.
            }
        };

        // Register event listener for all local task execution events.
        g.events().localListen(lsnr, EVTS_CACHE);

        // Generate cache events.
        for (int i = 0; i < 10; i++)
            g.cache(CACHE_NAME).put(i, String.valueOf(i));

        // Unsubscribe local task event listener.
        g.events().stopLocalListen(lsnr);
    }

    /**
     * Listen to events coming from all grid nodes.
     *
     * @throws GridException If failed.
     */
    private static void remoteListen() throws GridException {
        System.out.println();
        System.out.println(">>> Remote event listener example.");

        // This optional local callback is called for each event notification
        // that passed remote predicate listener.
        GridBiPredicate<UUID, GridCacheEvent> locLsnr = new GridBiPredicate<UUID, GridCacheEvent>() {
            @Override public boolean apply(UUID nodeId, GridCacheEvent evt) {
                System.out.println("Received cache event [evt=" + evt.name() + ", cacheName=" + evt.cacheName() +
                    ", key=" + evt.key() + ']');

                return true; // Return true to continue listening.
            }
        };

        // Remote filter which only accepts tasks whose name begins with "good-task" prefix.
        GridPredicate<GridCacheEvent> rmtLsnr = new GridPredicate<GridCacheEvent>() {
            @Override public boolean apply(GridCacheEvent evt) {
                Integer key = evt.key();

                return key != null && key % 2 == 0;
            }
        };

        Grid g = GridGain.grid();

        // Register event listeners on all nodes to listen for task events.
        GridFuture<UUID> fut = g.events().remoteListen(locLsnr, rmtLsnr, EVTS_CACHE);

        // Wait until event listeners are subscribed on all nodes.
        UUID lsnrId = fut.get();

        // Generate cache events.
        for (int i = 0; i < 10; i++)
            g.cache(CACHE_NAME).put(i, String.valueOf(i));

        g.events().stopRemoteListen(lsnrId);
    }
}
