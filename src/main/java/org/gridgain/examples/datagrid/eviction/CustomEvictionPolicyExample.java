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
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;

import java.util.*;
import java.util.Map.*;

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

            for (int i = 0; i < MAX_SIZE * 2; i++) {
                Employee e = new Employee(r.nextInt(20));

                System.out.println("Putting: " + e);

                g.cache(null).putx(i, e);
            }

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
        cc.setQueryIndexEnabled(true);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     *
     */
    public static class EvictionPolicy implements GridCacheEvictionPolicy<Integer, Employee> {
        /** */
        @GridInstanceResource
        private Grid g;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean b, GridCacheEntry<Integer, Employee> e) {
            int cnt = g.cache(null).size() - MAX_SIZE;

            if (!b && cnt > 0) {
                GridCache<Integer, Employee> cache = g.cache(null);

                GridCacheQuery<Entry<Integer, Employee>> q =
                    cache.queries().createSqlQuery(
                        Employee.class,
                        "from Employee order by Employee.prio asc limit ?");

                q.projection(g.forLocal());

                try {
                    Collection<Entry<Integer, Employee>> entries = q.execute(cnt).get();

                    for (Entry<Integer, Employee> e0 : entries) {
                        boolean res = cache.entry(e0.getKey()).evict();

                        System.out.println((res ? "Evicted: " : "Failed to evict: ") + e0.getValue());

                        if (cache.size() < MAX_SIZE)
                            return;
                    }
                }
                catch (GridException e1) {
                    log.error("Failed to execute query.", e1);
                }
            }
        }
    }

    /**
     *
     */
    public static class Employee {
        /** */
        @GridCacheQuerySqlField(index = true)
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
}
