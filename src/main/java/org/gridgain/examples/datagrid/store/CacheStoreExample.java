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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;

import javax.cache.configuration.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Demonstrates cache store with and without write-through.
 * <p>
 * This example is intended for custom cache store demonstration and is not supposed to run with remote nodes,
 * however, it can be launched in cluster. In order to do that make sure all nodes in topology have
 * {@link CacheMongoStore} and {@link Employee} on class path.
 */
public class CacheStoreExample {
    /** */
    private static final String CACHE_NAME = CacheStoreExample.class.getSimpleName();

    /**
     * Test cache store.
     *
     * @param args Nothing.
     */
    public static void main(String[] args) {
        // Disable quite logging.
        System.setProperty("GRIDGAIN_QUIET", "false");

        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            testStore(ignite, false);
            testStore(ignite, true);
        }
    }

    /**
     * Tests store with write-behind either turned on or off.
     *
     * @param writeBehind Write-behind flag.
     */
    private static void testStore(Ignite ignite, boolean writeBehind) {
        log(">>>");
        log(">>> Testing store with write-behind=" + writeBehind);
        log(">>>");

        CacheConfiguration<Long, Employee> cc = new CacheConfiguration<>(CACHE_NAME);

        cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheMongoStore.class));

        // Set write-behind flag.
        cc.setWriteBehindEnabled(writeBehind);

        try (IgniteCache<Long, Employee> cache = ignite.createCache(cc)) {
            atomicExample(ignite);
            transactionExample(ignite);
            lockExample(ignite);
        }
    }

    /**
     * Atomic example.
     *
     * @param ignite Ignite.
     */
    private static void atomicExample(Ignite ignite) {
        log(">>>");
        log(">>> Atomic example.");
        log(">>>");

        IgniteCache<Long, Employee> cache = ignite.cache(CACHE_NAME);

        IgniteLogger log = ignite.log().getLogger(CacheStoreExample.class);

        int cnt = 10;

        for (long i = 1; i <= cnt; i++)
            cache.put(i, new Employee(i, "Name-" + i, i * 1000));

        // Evict every other employee.
        for (long i = 1; i <= cnt; i += 2) {
            cache.clear(i);

            log(log, "Evicted key: " + i);
        }

        for (long i = 1; i <= cnt; i++)
            log(log, "Peeked at [key=" + i + ", val=" + cache.localPeek(i) + ']');

        for (long i = 1; i <= cnt; i++)
            log(log, "Got [key=" + i + ", val=" + cache.get(i) + ']');
    }

    /**
     * Transactional example which acquires pessimistic locks.
     *
     * @param ignite Ignite.
     */
    private static void transactionExample(Ignite ignite) {
        log(">>>");
        log(">>> Transactional example.");
        log(">>>");

        IgniteCache<Long, Employee> cache = ignite.cache(CACHE_NAME);

        IgniteLogger log = ignite.log().getLogger(CacheStoreExample.class);

        int cnt = 10;

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // This will acquire pessimistic lock on all accessed employees.
            for (long i = 1; i <= cnt; i++)
                cache.put(i, new Employee(i, "Name-" + i, i * 1000));

            // Evict every other employee.
            for (long i = 1; i <= cnt; i += 2) {
                cache.clear(i);

                log(log, "Evicted key: " + i);
            }

            for (long i = 1; i <= cnt; i++)
                log(log, "Peeked at [key=" + i + ", val=" + cache.localPeek(i) + ']');

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
     * @param ignite Ignite.
     */
    private static void lockExample(Ignite ignite) {
        log(">>>");
        log(">>> Lock example.");
        log(">>>");

        IgniteCache<Long, Employee> cache = ignite.cache(CACHE_NAME);

        IgniteLogger log = ignite.log().getLogger(CacheStoreExample.class);

        int cnt = 10;

        // Acquire a single lock.
        Lock lock = cache.lock(0L);

        lock.lock();

        try {
            for (long i = 1; i <= cnt; i++)
                cache.put(i, new Employee(i, "Name-" + i, i * 1000));

            // Evict every other employee.
            for (long i = 1; i <= cnt; i += 2) {
                cache.clear(i);

                log(log, "Evicted key: " + i);
            }

            for (long i = 1; i <= cnt; i++)
                log(log, "Peeked at [key=" + i + ", val=" + cache.localPeek(i) + ']');

            for (long i = 1; i <= cnt; i++)
                log(log, "Got [key=" + i + ", val=" + cache.get(i) + ']');
        }
        finally {
            lock.unlock();
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
    private static void log(IgniteLogger log, String msg) {
        if (log == null)
            System.out.println(msg);
        else
            log.info(msg);
    }
}
