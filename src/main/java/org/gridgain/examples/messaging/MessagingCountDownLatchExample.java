/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.messaging;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;

/**
 * Example that demonstrates how to exchange messages between nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}
 * or {@link CacheExampleNodeStartup} can be used.
 */
public final class MessagingCountDownLatchExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /** Latch name. */
    private static final String LATCH_NAME = "MessageLatch";

    /** Message topics. */
    private static final String TOPIC = "LATCH_TOPIC";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Messaging count down latch example started.");

            // Projection for remote nodes.
            GridProjection rmtPrj = g.forRemotes();

            GridCacheDataStructures dataStructures = g.cache(CACHE_NAME).dataStructures();

            int msgCnt = 20;

            // Register listeners on all grid nodes.
            UUID listenId = startListening(g, rmtPrj);

            GridCacheCountDownLatch latch = dataStructures.countDownLatch(
                LATCH_NAME,
                msgCnt,
                /*auto delete*/false,
                /*create*/true);

            try {
                assert latch != null;

                // Send unordered messages to all remote nodes.
                for (int i = 0; i < msgCnt; i++)
                    rmtPrj.forRandom().message().send(TOPIC, Integer.toString(i));

                System.out.println(">>> Finished sending messages. Waiting for all messages being processed.");

                latch.await();

                System.out.println(">>> Finished waiting for messages to process. " +
                    "Check output on all nodes for message printouts.");
            }
            finally {
                dataStructures.removeCountDownLatch(LATCH_NAME);

                rmtPrj.message().stopRemoteListen(listenId);
            }
        }
    }

    /**
     * Start listening to messages on all grid nodes within passed in projection.
     *
     * @param prj Grid projection.
     * @throws GridException If failed.
     */
    private static UUID startListening(final Grid g, GridProjection prj) throws GridException {
        // Add ordered message listener.
        return prj.message().remoteListen(TOPIC, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                try {
                    System.out.println("Received message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                    GridCacheCountDownLatch latch = g.cache(CACHE_NAME).dataStructures().countDownLatch(
                        LATCH_NAME, 0, false, false);

                    assert latch != null;

                    latch.countDown();

                    return true; // Return true to continue listening.
                }
                catch (GridException e) {
                    throw new GridClosureException(e);
                }
            }
        }).get();
    }
}
