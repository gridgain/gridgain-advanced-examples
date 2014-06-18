/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.nodelocal;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import java.util.concurrent.atomic.*;

/**
 * This example shows how to use NodeLocalMap.
 */
public class NodeLocalMapExample {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (final Grid g = GridGain.start("config/example-cache.xml")) {
            GridRunnable runnable = new GridRunnable() {
                @Override public void run() {
                    GridNodeLocalMap<String, AtomicInteger> locMap = g.nodeLocalMap();

                    AtomicInteger cntr = locMap.get("counter");

                    if (cntr == null)
                        cntr = locMap.addIfAbsent("counter", new AtomicInteger());

                    int execs = cntr.incrementAndGet();

                    System.out.println("Ran closure on this node " + execs + " time(s).");
                }
            };

            for (int i = 0; i < 5; i++)
                g.forRandom().compute().run(runnable).get();
        }
    }
}
