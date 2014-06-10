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

package org.gridgain.examples.datagrid.eviction;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;

import java.util.Map.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * This example shows:
 * <ol>
 *     <li>Have priority property on Employee.</li>
 *     <li>Load multiple employees to trigger eviction.</li>
 *     <li>Write a custom eviction policy that evict employees according to priority (write to log).</li>
 * </ol>
 *
 * To run this example against the cluster make sure that eviction policy is in class path of each node.
 */
public class CustomEvictionPolicyExample {
    /** */
    private static final int MAX_SIZE = 10;

    /**
     * @param args Args.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start(configuration())) {
            Random r = new Random();

            for (int i = 0; i < MAX_SIZE * 3; i++) {
                Employee e = new Employee(r.nextInt(20));

                System.out.println("Putting: " + e);

                g.cache(null).putx(i, e);
            }

            System.out.println("Employees in cache:");

            for (Employee e : g.<Integer, Employee>cache(null).values())
                System.out.println(e);
        }
    }

    /**
     * @return Configuration.
     */
    private static GridConfiguration configuration() {
        GridConfiguration c = new GridConfiguration();

        c.setLocalHost("127.0.0.1");

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setEvictionPolicy(new EvictionPolicy());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     *
     */
    public static class EvictionPolicy implements GridCacheEvictionPolicy<Integer, Employee> {
        /** */
        private static final String META = "evict-plc-META";

        /** */
        @GridInstanceResource
        private Grid g;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private final ConcurrentNavigableMap<PolicyKey, GridCacheEntry<Integer, Employee>> map =
            new ConcurrentSkipListMap<>();

        /** */
        private final AtomicLong seedGen = new AtomicLong();

        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, GridCacheEntry<Integer, Employee> e) {
            if (rmv) {
                PolicyKey key = e.meta(META);

                if (key != null)
                    map.remove(key, e);
            }
            else {
                Employee employee = e.peek();

                if (employee == null)
                    return;

                PolicyKey key = new PolicyKey(employee.priority(), seedGen.incrementAndGet());

                map.putIfAbsent(key, e);

                if (e.putMetaIfAbsent(META, key) != null || !e.isCached()) {
                    map.remove(key, e);

                    return;
                }

                // Map size has been increased. Need to check if evictions needed.
                int cnt = map.size() - MAX_SIZE;

                if (cnt > 0) {
                    for (Entry<PolicyKey, GridCacheEntry<Integer, Employee>> e0 : map.entrySet()) {
                        if (e0.getValue().evict()) {
                            map.remove(e0.getKey(), e0.getValue());

                            System.out.println("Evicted employee with prio: " + e0.getKey().prio);
                        }

                        cnt = map.size() - MAX_SIZE;

                        if (cnt <= 0)
                            return;
                    }
                }
            }
        }
    }

    /**
     *
     */
    public static class Employee {
        /** */
        private final int prio;

        /**
         * @param prio Priority.
         */
        private Employee(int prio) {
            this.prio = prio;
        }

        /**
         * @return priority.
         */
        int priority() {
            return prio;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Employee [prio=" + prio + ']';
        }
    }

    /**
     *
     */
    private static class PolicyKey implements Comparable<PolicyKey> {
        /** */
        private final int prio;

        /** */
        private final long seed;

        /**
         * @param prio Priority.
         * @param seed Seed.
         */
        private PolicyKey(int prio, long seed) {
            this.prio = prio;
            this.seed = seed;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(PolicyKey o) {
            return prio < o.prio ? -1 :
                prio > o.prio ? 1 :
                    seed < o.seed ? -1 : seed == o.seed ? 0 : 1;
        }
    }
}
