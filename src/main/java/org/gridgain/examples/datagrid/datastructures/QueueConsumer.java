/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;

/**
 * Consumer for queue example.
 */
public class QueueConsumer {
    /** Queue name. */
    public static final String QUEUE_NAME = "Queue";

    /** Maximum queue size. */
    public static final int QUEUE_SIZE = 10;

    /**
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            final IgniteQueue<String> queue = ignite.queue(QUEUE_NAME, QUEUE_SIZE, new CollectionConfiguration());

            assert queue != null;

            while (true) {
                String val = queue.take();

                System.out.println("Processing element taken from the queue: " + val);

                Thread.sleep(1000);
            }
        }
    }
}
