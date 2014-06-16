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

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import java.util.concurrent.atomic.*;

/**
 * Demonstrates how to use cache locks.
 */
public class CacheLockExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache lock example started.");

            final GridCache<Long, Long> cache = g.cache(CACHE_NAME);

            // Lock the key. Timeout 0 = infinite.
            cache.lock(1L, 0);

            final AtomicReference<Object> res = new AtomicReference<>();

            new Thread(new Runnable() {
                @Override public void run() {
                    System.out.println("Will try to lock key with 500 ms timeout (should fail).");

                    try {
                        boolean b = cache.lock(1L, 500);

                        if (!b)
                            res.set(false);
                        else
                            // Cannot reach this line.
                            System.exit(1);
                    }
                    catch (GridException e) {
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
            cache.unlock(1L);

            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    System.out.println("Will try to lock key with 500 ms timeout (should fail).");

                    try {
                        cache.lock(1L, 0);

                        res.set(true);
                    }
                    catch (GridException e) {
                        e.printStackTrace();

                        System.exit(1);
                    }
                    finally {
                        try {
                            cache.unlock(1L);
                        }
                        catch (GridException e) {
                            e.printStackTrace();
                        }
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
