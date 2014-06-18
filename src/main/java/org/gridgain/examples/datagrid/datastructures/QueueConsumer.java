/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;

/**
 * Consumer for queue example.
 */
public class QueueConsumer {
    /** Cache name. */
    public static final String CACHE_NAME = "partitioned_tx";

    /** Queue name. */
    public static final String QUEUE_NAME = "Queue";

    /** Maximum queue size. */
    public static final int QUEUE_SIZE = 10;

    /**
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            final GridCacheQueue<String> queue = g.cache(CACHE_NAME).dataStructures().queue(QUEUE_NAME, QUEUE_SIZE,
                /*colocated*/false, /*create*/true);

            assert queue != null;

            while (true) {
                String val = queue.take();

                System.out.println("Processing element taken from the queue: " + val);

                Thread.sleep(1000);
            }
        }
    }
}
