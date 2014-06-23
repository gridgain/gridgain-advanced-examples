/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;

/**
 * Example shows how to use AtomicLong.
 */
public class AtomicLongExample {
    /** Cache name. */
    public static final String CACHE_NAME = "partitioned_tx";

    /** Execution counter. */
    public static final String ATOMIC_LONG_NAME = "ExecCounter";

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (final Grid g = GridGain.start("config/example-cache.xml")) {
            final GridCache<Object, Object> cache = g.cache(CACHE_NAME);

            // Clear caches before running example.
            cache.globalClearAll();

            GridCacheDataStructures ds = cache.dataStructures();

            GridCacheAtomicLong cntr = ds.atomicLong(ATOMIC_LONG_NAME, 0, /*create*/true);

            try {
                assert cntr != null;

                GridRunnable runnable = new GridRunnable() {
                    @Override public void run() {
                        try {
                            GridCacheAtomicLong cntr = cache.dataStructures().atomicLong(
                                ATOMIC_LONG_NAME, 0, /*create*/false);

                            assert cntr != null;

                            cntr.incrementAndGet();
                        }
                        catch (GridException e) {
                            throw new GridClosureException(e);
                        }
                    }
                };

                int cnt = 50;

                for (int i = 0; i < cnt; i++)
                    g.forRandom().compute().run(runnable).get();

                System.out.println("Execution count [expected=" + cnt + ", actual=" + cntr.get() + ']');
            }
            finally {
                ds.removeAtomicLong(ATOMIC_LONG_NAME);
            }
        }
    }
}
