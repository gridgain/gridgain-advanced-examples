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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;

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
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            ClusterGroup producers = ignite.cluster().forAttribute(NODE_ROLE, ROLE_PRODUCER);
            ClusterGroup consumers = ignite.cluster().forAttribute(NODE_ROLE, ROLE_CONSUMER);

            // Deploy at most 1 producer for the cluster.
            ignite.services(producers).deployClusterSingleton("producer", new CacheQueueProducerService());

            // Deploy total of 2 consumer services with maximum of 1 per node.
            ignite.services(consumers).deployMultiple("consumer", new CacheQueueConsumerService(), 2, 1);

            System.in.read();
        }
    }
}
