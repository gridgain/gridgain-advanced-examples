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

package org.gridgain.examples.datagrid.lock;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.gridgain.examples.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Demonstrates how to use cache locks.
 * <p>
 * Remote nodes should always be started with special configuration file:
 * {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}
 * or {@link ExampleNodeStartup} can be used.
 */
public class CacheLockExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheLockExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache lock example started.");

            CacheConfiguration<Long, Long> cc = new CacheConfiguration<>(CACHE_NAME);

            cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            try (IgniteCache<Long, Long> cache = ignite.createCache(cc)) {
                Lock lock = cache.lock(1L);

                // Lock the key.
                lock.lock();

                final AtomicReference<Object> res = new AtomicReference<>();

                new Thread(new Runnable() {
                    @Override public void run() {
                        System.out.println("Will try to lock key with 500 ms timeout (should fail).");

                        try {
                            boolean b = cache.lock(1L).tryLock(500, TimeUnit.MILLISECONDS);

                            if (!b)
                                res.set(false);
                            else
                                // Cannot reach this line.
                                System.exit(1);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();

                            System.exit(1);
                        }
                    }
                }).start();

                while (res.get() == null) {
                    System.out.println("Waiting for operation to complete in parallel thread.");

                    Thread.sleep(500);
                }

                if (res.get() instanceof Boolean)
                    System.out.println("Locking in parallel thread failed: " + res.get());
                else
                    throw new Exception("Unexpected result: " + res.get());

                res.set(null);

                // Unlock the key.
                lock.unlock();

                Thread thread = new Thread(new Runnable() {
                    @Override public void run() {
                        System.out.println("Will try to lock key with 500 ms timeout (should fail).");

                        Lock lock0 = cache.lock(1L);

                        try {
                            lock0.lock();

                            res.set(true);
                        }
                        finally {
                            lock0.unlock();
                        }
                    }
                });

                thread.start();

                while (res.get() == null) {
                    System.out.println("Waiting for operation to complete in parallel thread.");

                    Thread.sleep(500);
                }

                if (res.get() instanceof Boolean)
                    System.out.println("Locking in parallel thread succeeded: " + res.get());
                else
                    throw new Exception("Unexpected result: " + res.get());

                // Let thread unlock the key before stopping the grid.
                thread.join();

                System.out.println(">>> Cache lock example finished.");
            }
        }
    }
}
