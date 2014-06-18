/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.messaging;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Example that demonstrates how to exchange messages between nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}
 * or {@link CacheExampleNodeStartup} can be used.
 */
public final class MessagingUnorderedExample {
    /** Message topics. */
    private static final String TOPIC = "UNORDERED";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Unordered messaging example started.");

            // Projection for remote nodes.
            GridProjection rmtPrj = g.forRemotes();

            int msgCnt = 10;

            // Register listeners on all grid nodes.
            startListening(rmtPrj);

            // Send unordered messages to all remote nodes.
            for (int i = 0; i < msgCnt; i++)
                rmtPrj.message().send(TOPIC, Integer.toString(i));

            System.out.println(">>> Finished sending unordered messages. Check output on all nodes for messages " +
                "printouts.");
        }
    }

    /**
     * Start listening to messages on all grid nodes within passed in projection.
     *
     * @param prj Grid projection.
     * @throws GridException If failed.
     */
    private static void startListening(GridProjection prj) throws GridException {
        // Add ordered message listener.
        prj.message().remoteListen(TOPIC, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                return true; // Return true to continue listening.
            }
        }).get();
    }
}
