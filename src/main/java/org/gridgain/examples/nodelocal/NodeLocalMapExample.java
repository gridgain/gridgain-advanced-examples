/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.nodelocal;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;

import java.util.*;
import java.util.concurrent.*;
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
        try (final Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            IgniteRunnable runnable = new IgniteRunnable() {
                @Override public void run() {
                    ConcurrentMap<String, AtomicInteger> locMap = ignite.cluster().nodeLocalMap();

                    AtomicInteger cntr = locMap.get(COUNTER_KEY);

                    if (cntr == null) {
                        AtomicInteger old = locMap.putIfAbsent(COUNTER_KEY, cntr = new AtomicInteger());

                        if (old != null)
                            cntr = old;
                    }

                    int execs = cntr.incrementAndGet();

                    System.out.println("Ran closure on this node " + execs + " time(s).");
                }
            };

            int execCnt = 10;

            for (int i = 0; i < execCnt; i++)
                ignite.compute(ignite.cluster().forRandom()).run(runnable);

            Collection<Integer> col = ignite.compute().broadcast(new IgniteCallable<Integer>() {
                @Override public Integer call() throws Exception {
                    ConcurrentMap<String, AtomicInteger> locMap = ignite.cluster().nodeLocalMap();

                    AtomicInteger cnt = locMap.get(COUNTER_KEY);

                    return cnt == null ? 0 : cnt.get();
                }
            });

            int sum = 0;

            for (Integer c : col)
                sum += c;

            System.out.println("Execution count [expected=" + execCnt + ", actual=" + sum + ']');
        }
    }
}
