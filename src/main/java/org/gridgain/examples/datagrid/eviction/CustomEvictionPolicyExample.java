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

package org.gridgain.examples.datagrid.eviction;

import org.apache.ignite.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.configuration.*;

import javax.cache.*;
import java.io.*;
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
 * <p>
 * This example is intended for custom eviction policy demonstration and is not supposed to run with remote nodes,
 * however, it can be launched in cluster. In order to do that make sure all nodes in topology have
 * {@link EvictionPolicy} on class path.
 */
public class CustomEvictionPolicyExample {
    /** */
    private static final String CACHE_NAME = CustomEvictionPolicyExample.class.getSimpleName();

    /** */
    private static final int MAX_SIZE = 10;

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            Random r = new Random();

            CacheConfiguration<Integer, Employee> cc = new CacheConfiguration<>(CACHE_NAME);

            cc.setEvictionPolicy(new EmployeeEvictionPolicy());

            try (IgniteCache<Integer, Employee> cache = ignite.createCache(cc)) {
                for (int i = 0; i < MAX_SIZE * 3; i++) {
                    Employee e = new Employee(r.nextInt(20));

                    System.out.println("Putting: " + e);

                    cache.put(i, e);
                }

                System.out.println("Employees in cache:");

                for (Cache.Entry<Integer, Employee> e : cache)
                    System.out.println(e.getValue());
            }
        }
    }

    /**
     * Custom eviction policy which maintains Employees put to cache
     * according to their priority.
     */
    public static class EmployeeEvictionPolicy implements EvictionPolicy<Integer, Employee>, Serializable {
        /** Counter to avoid non-constant ConcurrentMap.size(). */
        private final AtomicLong mapSize = new AtomicLong();

        /** Sorted map to maintain employees in priority order. */
        private final ConcurrentNavigableMap<PolicyKey, EvictableEntry<Integer, Employee>> map =
            new ConcurrentSkipListMap<>();

        /** Seed generator to avoid collisions on same prio. */
        private final AtomicLong seedGen = new AtomicLong();

        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, EvictableEntry<Integer, Employee> e) {
            if (rmv) {
                PolicyKey key = e.meta();

                if (key != null && map.remove(key, e))
                    mapSize.decrementAndGet();
            }
            else {
                Employee employee = e.getValue();

                if (employee == null)
                    return;

                PolicyKey key = new PolicyKey(employee.priority(), seedGen.incrementAndGet());

                // This put always succeeds (key is unique).
                map.putIfAbsent(key, e);

                mapSize.incrementAndGet();

                // If another thread is processing the same entry, then undo and return.
                if (e.putMetaIfAbsent(key) != null || !e.isCached()) {
                    if (map.remove(key, e))
                        mapSize.decrementAndGet();

                    return;
                }

                // At this point, the map size has been increased.
                // Need to check if evictions are needed.
                long cnt = mapSize.get() - MAX_SIZE;

                if (cnt > 0) {
                    for (Entry<PolicyKey, EvictableEntry<Integer, Employee>> e0 : map.entrySet()) {
                        // If successfully evicted.
                        if (e0.getValue().evict() && map.remove(e0.getKey(), e0.getValue())) {
                            mapSize.decrementAndGet();

                            System.out.println("Evicted employee with priority: " + e0.getKey().prio);
                        }

                        cnt = mapSize.get() - MAX_SIZE;

                        // If we evicted required number of entries, return.
                        if (cnt <= 0)
                            return;
                    }
                }
            }
        }
    }

    /**
     * Employee.
     */
    public static class Employee implements Serializable {
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
            return "Employee [priority=" + prio + ']';
        }
    }

    /**
     * Eviction policy key. Must be comparable based on priority.
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
