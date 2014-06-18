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

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * This example shows how to use NodeLocalMap.
 */
public class NodeLocalMapExample {
    /** Key for node local map. */
    public static final String COUNTER_KEY = "counter";

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (final Grid g = GridGain.start("config/example-cache.xml")) {
            GridRunnable runnable = new GridRunnable() {
                @Override public void run() {
                    GridNodeLocalMap<String, AtomicInteger> locMap = g.nodeLocalMap();

                    AtomicInteger cntr = locMap.get(COUNTER_KEY);

                    if (cntr == null)
                        cntr = locMap.addIfAbsent(COUNTER_KEY, new AtomicInteger());

                    int execs = cntr.incrementAndGet();

                    System.out.println("Ran closure on this node " + execs + " time(s).");
                }
            };

            int execCnt = 10;

            for (int i = 0; i < execCnt; i++)
                g.forRandom().compute().run(runnable).get();

            GridFuture<Collection<Integer>> qryFut = g.compute().broadcast(new GridCallable<Integer>() {
                @Override public Integer call() throws Exception {
                    GridNodeLocalMap<String, AtomicInteger> locMap = g.nodeLocalMap();

                    AtomicInteger cnt = locMap.get(COUNTER_KEY);

                    return cnt == null ? 0 : cnt.get();
                }
            });

            int sum = 0;

            for (Integer c : qryFut.get())
                sum += c;

            System.out.println("Execution count [expected=" + execCnt + ", actual=" + sum + ']');
        }
    }
}
