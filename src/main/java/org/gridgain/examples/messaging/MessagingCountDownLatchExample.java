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
    private enum TOPIC { ORDERED, UNORDERED }

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Messaging example started.");

            // Projection for remote nodes.
            GridProjection rmtPrj = g.forRemotes();

            int rmtNodes = rmtPrj.nodes().size();

            if (rmtNodes == 0) {
                System.out.println("Start one or more remote nodes to run this example.");

                return;
            }

            GridCacheDataStructures dataStructures = g.cache(CACHE_NAME).dataStructures();

            int unorderedMsgCnt = 10;
            int orderedMsgCnt = 10;

            GridCacheCountDownLatch latch = dataStructures.countDownLatch(LATCH_NAME,
                rmtNodes * (unorderedMsgCnt + orderedMsgCnt), /*auto delete*/false, true);

            try {
                assert latch != null;

                // Register listeners on all grid nodes.
                startListening(g, rmtPrj);

                // Send unordered messages to all remote nodes.
                for (int i = 0; i < unorderedMsgCnt; i++)
                    rmtPrj.message().send(TOPIC.UNORDERED, Integer.toString(i));

                System.out.println(">>> Finished sending unordered messages.");

                // Send ordered messages to all remote nodes.
                for (int i = 0; i < orderedMsgCnt; i++)
                    rmtPrj.message().sendOrdered(TOPIC.ORDERED, Integer.toString(i), 0);

                System.out.println(">>> Finished sending ordered messages. Waiting for all messages being processed.");

                latch.await();

                System.out.println(">>> Finished waiting for messages to process. " +
                    "Check output on all nodes for message printouts.");
            }
            finally {
                dataStructures.removeCountDownLatch(LATCH_NAME);
            }
        }
    }

    /**
     * Start listening to messages on all grid nodes within passed in projection.
     *
     * @param prj Grid projection.
     * @throws GridException If failed.
     */
    private static void startListening(final Grid g, GridProjection prj) throws GridException {
        // Add ordered message listener.
        prj.message().remoteListen(TOPIC.ORDERED, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                try {
                    System.out.println("Received ordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

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

        // Add unordered message listener.
        prj.message().remoteListen(TOPIC.UNORDERED, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                try {
                    System.out.println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

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
