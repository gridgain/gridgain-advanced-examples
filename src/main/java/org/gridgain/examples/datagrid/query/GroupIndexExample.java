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

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;

import java.io.*;
import java.util.*;

/**
 * This examples shows usage of group indexes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}.
 */
public class GroupIndexExample {
    /** Employees cache name. */
    private static final String EMPLOYEES_CACHE_NAME = "partitioned";

    /** Organizations cache name. */
    private static final String ORG_CACHE_NAME = "replicated";

    /**
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            initialize();

            GridCacheProjection<UUID, Employee> employeeCache = GridGain.grid().cache(EMPLOYEES_CACHE_NAME);

            GridCacheQuery<?> q = employeeCache.queries().createSqlFieldsQuery(
                "select Employee.firstName, Employee.lastName, Employee.salary, Organization.name " +
                    "from \"partitioned\".Employee, \"replicated\".Organization " +
                    "where Employee.orgId=Organization.id and Employee.salary > 1000");

            System.out.println("Query results: ");
            System.out.println(q.execute().get());
        }
    }

    /**
     * Populate cache with test data.
     *
     * @throws GridException In case of error.
     * @throws InterruptedException In case of error.
     */
    private static void initialize() throws GridException, InterruptedException {
        GridCacheProjection<UUID, Organization> orgCache = GridGain.grid().cache(ORG_CACHE_NAME);

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.id, org1);
        orgCache.put(org2.id, org2);

        // Employees will be collocated with their organizations since
        // Organizations are stored in replicated cache.
        GridCacheProjection<UUID, Employee> employeeCache = GridGain.grid().cache(EMPLOYEES_CACHE_NAME);

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
    @GridCacheQueryGroupIndex.List(
        @GridCacheQueryGroupIndex(name = "idx1") // Find employee of organization with passed salary.
    )
    private static class Employee implements Serializable {
        /** Last name. */
        @GridCacheQuerySqlField(index = true)
        private String firstName;

        /** Last name. */
        @GridCacheQuerySqlField(index = true)
        private String lastName;

        /** Organization ID (indexed). */
        @GridCacheQuerySqlField(index = true)
        @GridCacheQuerySqlField.Group(name = "idx1", order = 0)
        private UUID orgId;

        /** Salary (indexed). */
        @GridCacheQuerySqlField(index = true)
        @GridCacheQuerySqlField.Group(name = "idx1", order = 1)
        private double salary;

        /**
         * @param firstName First name.
         * @param lastName Last name.
         * @param orgId Organization ID.
         * @param salary Salary.
         */
        private Employee(String firstName, String lastName, UUID orgId, double salary) {
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
        @GridCacheQuerySqlField(index = true)
        private UUID id;

        /** Organization name (indexed). */
        @GridCacheQuerySqlField(index = true)
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
