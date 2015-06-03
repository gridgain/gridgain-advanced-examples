/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.messaging;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.examples.*;

import java.util.*;

/**
 * Example that demonstrates how to exchange messages between nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}
 * or {@link ExampleNodeStartup} can be used.
 */
public final class MessagingCountDownLatchExample {
    /** Latch name. */
    private static final String LATCH_NAME = "MessageLatch";

    /** Message topics. */
    private static final String TOPIC = "LATCH_TOPIC";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Messaging count down latch example started.");

            // Cluster group for remote nodes.
            ClusterGroup remotes = ignite.cluster().forRemotes();

            int msgCnt = 20;

            // Register listeners on all grid nodes.
            UUID listenId = startListening(ignite, remotes);

            IgniteCountDownLatch latch = ignite.countDownLatch(
                LATCH_NAME,
                msgCnt,
                /*auto delete*/false,
                /*create*/true);

            assert latch != null;

            try {
                // Send unordered messages to all remote nodes.
                for (int i = 0; i < msgCnt; i++)
                    ignite.message(remotes.forRandom()).send(TOPIC, Integer.toString(i));

                System.out.println(">>> Finished sending messages. Waiting for all messages being processed.");

                latch.await();

                System.out.println(">>> Finished waiting for messages to process. " +
                    "Check output on all nodes for message printouts.");
            }
            finally {
                latch.close();

                ignite.message(remotes).stopRemoteListen(listenId);
            }
        }
    }

    /**
     * Start listening to messages on all grid nodes within passed in projection.
     *
     * @param ignite Ignite.
     * @param grp Cluster group.
     */
    private static UUID startListening(final Ignite ignite, ClusterGroup grp) {
        // Add ordered message listener.
        return ignite.message(grp).remoteListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                IgniteCountDownLatch latch = ignite.countDownLatch(LATCH_NAME, 0, false, false);

                assert latch != null;

                latch.countDown();

                return true; // Return true to continue listening.
            }
        });
    }
}
