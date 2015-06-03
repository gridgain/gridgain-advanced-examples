/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;

/**
 * Example shows how to use AtomicLong.
 */
public class AtomicLongExample {
    /** Execution counter. */
    public static final String ATOMIC_LONG_NAME = "ExecCounter";

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (final Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            IgniteAtomicLong cntr = ignite.atomicLong(ATOMIC_LONG_NAME, 0, /*create*/true);

            assert cntr != null;

            try {
                IgniteRunnable runnable = new IgniteRunnable() {
                    @Override public void run() {
                        IgniteAtomicLong cntr = ignite.atomicLong(ATOMIC_LONG_NAME, 0, /*create*/false);

                        assert cntr != null;

                        cntr.incrementAndGet();
                    }
                };

                int cnt = 50;

                for (int i = 0; i < cnt; i++) {
                    ClusterGroup grp = ignite.cluster().forRandom();

                    ignite.compute(grp).run(runnable);
                }

                System.out.println("Execution count [expected=" + cnt + ", actual=" + cntr.get() + ']');
            }
            finally {
                cntr.close();
            }
        }
    }
}
