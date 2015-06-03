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

import static org.gridgain.examples.datagrid.datastructures.QueueConsumer.*;

/**
 * Producer node for queue example
 */
public class QueueProducer {
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            final IgniteQueue<String> queue = ignite.queue(QUEUE_NAME, QUEUE_SIZE, new CollectionConfiguration());

            assert queue != null;

            int cnt = 0;

            while (true) {
                String val = String.valueOf(cnt++);

                queue.put(val);

                System.out.println("Inserted element to the queue for processing: " + val);
            }
        }
    }
}
