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

package org.gridgain.examples.datagrid.store;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.logger.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Demonstrates cache store with and without write-through.
 * <p>
 * This example is intended for custom cache store demonstration and is not supposed to run with remote nodes,
 * however, it can be launched in cluster. In order to do that make sure all nodes in topology have
 * {@link CacheMongoStore} and {@link Employee} on class path.
 */
public class CacheStoreExample {
    /**
     * Test cache store.
     *
     * @param args Nothing.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        // Disable quite logging.
        System.setProperty("GRIDGAIN_QUIET", "false");

        testStore(false);
        testStore(true);
    }

    /**
     * Tests store with write-behind either turned on or off.
     *
     * @param writeBehind Write-behind flag.
     * @throws GridException If failed.
     */
    private static void testStore(boolean writeBehind) throws GridException {
        log(">>>");
        log(">>> Testing store with write-behind=" + writeBehind);
        log(">>>");

        GridConfiguration c = new GridConfiguration();

        c.setLocalHost("localhost");

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setName("test");
        cc.setAtomicityMode(GridCacheAtomicityMode.TRANSACTIONAL);
        cc.setCacheMode(GridCacheMode.PARTITIONED);
        cc.setStore(new CacheMongoStore());

        // Set write-behind flag.
        cc.setWriteBehindEnabled(writeBehind);
        cc.setWriteBehindFlushFrequency(3000);

        c.setCacheConfiguration(cc);

        try (Grid g = GridGain.start(c)) {
            atomicExample(g);
            transactionExample(g);
            lockExample(g);
        }
    }

    /**
     * Atomic example.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void atomicExample(Grid g) throws GridException {
        log(">>>");
        log(">>> Atomic example.");
        log(">>>");

        GridCache<Long, Employee> cache = g.cache("test");

        GridLogger log = g.log().getLogger(CacheStoreExample.class);

        int cnt = 10;

        for (long i = 1; i <= cnt; i++)
            cache.putx(i, new Employee(i, "Name-" + i, i * 1000));

        // Evict every other employee.
        for (long i = 1; i <= cnt; i+= 2) {
            cache.evict(i);

            log(log, "Evicted key: " + i);
        }

        for (long i = 1; i <= cnt; i++)
            log(log, "Peeked at [key=" + i + ", val=" + cache.peek(i) + ']');

        for (long i = 1; i <= cnt; i++)
            log(log, "Got [key=" + i + ", val=" + cache.get(i) + ']');
    }

    /**
     * Transactional example which acquires pessimistic locks.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void transactionExample(Grid g) throws GridException {
        log(">>>");
        log(">>> Transactional example.");
        log(">>>");

        GridCache<Long, Employee> cache = g.cache("test");

        GridLogger log = g.log().getLogger(CacheStoreExample.class);

        int cnt = 10;

        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // This will acquire pessimistic lock on all accessed employees.
            for (long i = 1; i <= cnt; i++)
                cache.putx(i, new Employee(i, "Name-" + i, i * 1000));

            // Evict every other employee.
            for (long i = 1; i <= cnt; i += 2) {
                cache.evict(i);

                log(log, "Evicted key: " + i);
            }

            for (long i = 1; i <= cnt; i++)
                log(log, "Peeked at [key=" + i + ", val=" + cache.peek(i) + ']');

            for (long i = 1; i <= cnt; i++)
                log(log, "Got [key=" + i + ", val=" + cache.get(i) + ']');

            // Commit and release locks.
            tx.commit();
        }
    }

    /**
     * Lock example which acquires a lock on cache and performs all operations
     * under a single lock.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void lockExample(Grid g) throws GridException {
        log(">>>");
        log(">>> Lock example.");
        log(">>>");

        GridCache<Long, Employee> cache = g.cache("test");

        GridLogger log = g.log().getLogger(CacheStoreExample.class);

        int cnt = 10;

        // Acquire a single lock.
        cache.lock(/*key*/0L, /*timeout*/0L);

        try {
            for (long i = 1; i <= cnt; i++)
                cache.putx(i, new Employee(i, "Name-" + i, i * 1000));

            // Evict every other employee.
            for (long i = 1; i <= cnt; i += 2) {
                cache.evict(i);

                log(log, "Evicted key: " + i);
            }

            for (long i = 1; i <= cnt; i++)
                log(log, "Peeked at [key=" + i + ", val=" + cache.peek(i) + ']');

            for (long i = 1; i <= cnt; i++)
                log(log, "Got [key=" + i + ", val=" + cache.get(i) + ']');
        }
        finally {
            cache.unlock(/*key*/0L);
        }
    }

    /**
     * Logs message.
     *
     * @param msg Message to log.
     */
    private static void log(String msg) {
        log(null, msg);
    }

    /**
     * Logs message.
     *
     * @param log Log.
     * @param msg Message to log.
     */
    private static void log(GridLogger log, String msg) {
        if (log == null)
            System.out.println(msg);
        else
            log.info(msg);
    }
}
