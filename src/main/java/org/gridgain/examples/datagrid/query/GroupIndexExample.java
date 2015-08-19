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

package org.gridgain.examples.datagrid.query;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;

import java.io.*;
import java.util.*;

/**
 * This examples shows usage of group indexes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}.
 */
public class GroupIndexExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE_NAME = GroupIndexExample.class.getSimpleName() + "-organizations";

    /** Employees cache name. */
    private static final String EMPLOYEES_CACHE_NAME = GroupIndexExample.class.getSimpleName() + "-employees";

    /**
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            CacheConfiguration<UUID, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE_NAME);

            orgCacheCfg.setCacheMode(CacheMode.REPLICATED);
            orgCacheCfg.setIndexedTypes(UUID.class, Organization.class);

            CacheConfiguration<UUID, Employee> empCacheCfg = new CacheConfiguration<>(EMPLOYEES_CACHE_NAME);

            empCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            empCacheCfg.setIndexedTypes(UUID.class, Employee.class);

            try (
                IgniteCache<UUID, Organization> orgCache = ignite.createCache(orgCacheCfg);
                IgniteCache<UUID, Employee> empCache = ignite.createCache(empCacheCfg)
            ) {
                initialize(ignite);

                SqlFieldsQuery q = new SqlFieldsQuery(
                    "select e.firstName, e.lastName, e.salary, o.name " +
                    "from Employee e, \"" + ORG_CACHE_NAME + "\".Organization o " +
                    "where e.orgId = o.id and e.salary = 1000 and o.name = 'Other'");

                System.out.println("Query results: ");
                System.out.println(empCache.query(q).getAll());
            }
        }
    }

    /**
     * Populate cache with test data.
     *
     * @throws InterruptedException In case of error.
     */
    private static void initialize(Ignite ignite) throws InterruptedException {
        IgniteCache<UUID, Organization> orgCache = ignite.cache(ORG_CACHE_NAME);
        IgniteCache<UUID, Employee> employeeCache = ignite.cache(EMPLOYEES_CACHE_NAME);

        // Clear caches before running example.
        employeeCache.clear();
        orgCache.clear();

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.id, org1);
        orgCache.put(org2.id, org2);

        // People.
        Employee p1 = new Employee("John", "Doe", org1.id, 2000);
        Employee p2 = new Employee("Jane", "Doe", org1.id, 1000);
        Employee p3 = new Employee("John", "Smith", org2.id, 1000);
        Employee p4 = new Employee("Jane", "Smith", org2.id, 2000);

        employeeCache.put(UUID.randomUUID(), p1);
        employeeCache.put(UUID.randomUUID(), p2);
        employeeCache.put(UUID.randomUUID(), p3);
        employeeCache.put(UUID.randomUUID(), p4);

        // Wait 1 second to be sure that all nodes processed put requests.
        Thread.sleep(1000);
    }


    /**
     * Employee.
     */
    private static class Employee implements Serializable {
        /** Last name. */
        @QuerySqlField(index = true)
        private String firstName;

        /** Last name. */
        @QuerySqlField(index = true)
        private String lastName;

        /** Organization ID (group indexed). */
        @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "idx1", order = 0)})
        private UUID orgId;

        /** Salary (indexed and group indexed). */
        @QuerySqlField(index = true,
            orderedGroups = {@QuerySqlField.Group(name = "idx1", order = 1)})
        private int salary;

        /**
         * @param firstName First name.
         * @param lastName Last name.
         * @param orgId Organization ID.
         * @param salary Salary.
         */
        private Employee(String firstName, String lastName, UUID orgId, int salary) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.orgId = orgId;
            this.salary = salary;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [firstName=" + firstName +
                ", lastName=" + lastName +
                ", orgId=" + orgId +
                ", salary=" + salary + ']';
        }
    }

    /**
     * Organization.
     */
    private static class Organization implements Serializable {
        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private UUID id;

        /** Organization name (indexed). */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Create organization.
         *
         * @param name Organization name.
         */
        Organization(String name) {
            id = UUID.randomUUID();

            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id + ", name=" + name + ']';
        }
    }
}
