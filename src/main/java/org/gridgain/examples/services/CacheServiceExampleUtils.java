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

package org.gridgain.examples.services;

import org.gridgain.grid.*;

/**
 * Configuration constants for this example.
 */
public class CacheServiceExampleUtils {
    /** Attribute name for node role. */
    public static final String NODE_ROLE = "NODE_ROLE";

    /** Consumer node role. */
    public static final String ROLE_CONSUMER = "CONSUMER";

    /** Producer node role. */
    public static final String ROLE_PRODUCER = "PRODUCER";

    /** Cache name. */
    public static final String CACHE_NAME = "partitioned_tx";

    /** Queue name. */
    public static final String QUEUE_NAME = "example-queue";

    /** Queue size. */
    public static final int QUEUE_SIZE = 10;

    /**
     * Start GridGain node and deploy producer and consumer services.
     *
     * @throws Exception If failed.
     */
    public static void runGridNode() throws Exception {
        try (Grid grid = GridGain.start("examples/config/example-cache.xml")) {
            GridProjection producers = grid.forAttribute(NODE_ROLE, ROLE_PRODUCER);
            GridProjection consumers = grid.forAttribute(NODE_ROLE, ROLE_CONSUMER);

            // Deploy at most 1 producer for the cluster.
            producers.services().deployClusterSingleton("producer", new CacheQueueProducerService());

            // Deploy total of 2 consumer services with maximum of 1 per node.
            consumers.services().deployMultiple("consumer", new CacheQueueConsumerService(), 2, 1);

            System.in.read();
        }
    }
}
