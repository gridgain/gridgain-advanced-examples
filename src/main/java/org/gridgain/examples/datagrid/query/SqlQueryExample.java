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
import org.apache.ignite.configuration.*;
import org.gridgain.examples.*;
import org.gridgain.examples.model.*;

import java.util.*;

/**
 * This example shows SQL query usage.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}
 * or {@link ExampleNodeStartup} can be used.
 */
public class SqlQueryExample {
    /** Replicated cache name (to store organizations). */
    private static final String ORG_CACHE_NAME = SqlQueryExample.class.getSimpleName() + "-organizations";

    /** Partitioned cache name (to store employees). */
    private static final String PERSON_CACHE_NAME = SqlQueryExample.class.getSimpleName() + "-persons";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache query example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE_NAME);

            orgCacheCfg.setCacheMode(CacheMode.REPLICATED);
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<PersonKey, Person> personCacheCfg = new CacheConfiguration<>(PERSON_CACHE_NAME);

            personCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            personCacheCfg.setIndexedTypes(PersonKey.class, Person.class);

            try (
                IgniteCache<Long, Organization> orgCache = ignite.createCache(orgCacheCfg);
                IgniteCache<PersonKey, Person> personCache = ignite.createCache(personCacheCfg)
            ) {
                // Populate cache.
                initialize();

                // Example for SQL-based querying employees based on salary ranges.
                sqlQuery();

                // Example for SQL-based querying employees for a given organization (includes SQL join).
                sqlQueryWithJoin();

                // Example for SQL-based fields queries that return only required
                // fields instead of whole key-value pairs.
                sqlFieldsQuery();

                // Example for SQL-based fields queries that uses joins.
                sqlFieldsQueryWithJoin();

                // Example for query that uses aggregation.
                aggregateQuery();

                // Example for query that uses grouping.
                groupByQuery();

                // Full text query example.
                textQuery();

                print("Cache query example finished.");
            }
        }
    }

    /**
     * Example for SQL queries based on salary ranges.
     */
    private static void sqlQuery() {
        IgniteCache<PersonKey, Person> cache = Ignition.ignite().cache(PERSON_CACHE_NAME);

        // Create query which selects salaries based on range.
        SqlQuery<PersonKey, Person> qry = new SqlQuery<>(Person.class, "salary > ? and salary <= ?");

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000: ", cache.query(qry.setArgs(0, 1000)).getAll());

        print("People with salaries between 1000 and 2000: ", cache.query(qry.setArgs(1000, 2000)).getAll());

        print("People with salaries greater than 2000: ", cache.query(qry.setArgs(2000, Integer.MAX_VALUE)).getAll());
    }

    /**
     * Example for SQL queries based on all employees working for a specific organization.
     */
    private static void sqlQueryWithJoin() {
        IgniteCache<PersonKey, Person> cache = Ignition.ignite().cache(PERSON_CACHE_NAME);

        // Create query which joins on 2 types to select people for a specific organization.
        SqlQuery<PersonKey, Person> qry = new SqlQuery<>(Person.class,
                "from Person, \"" + ORG_CACHE_NAME + "\".Organization " +
                "where Person.orgId = Organization.id " +
                "and lower(Organization.name) = lower(?)");

        // Execute queries for find employees for different organizations.
        print("Following people are 'GridGain' employees (SQL join): ", cache.query(qry.setArgs("GridGain")).getAll());
        print("Following people are 'Other' employees (SQL join): ", cache.query(qry.setArgs("Other")).getAll());
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void sqlFieldsQuery() {
        IgniteCache<?, ?> cache = Ignition.ignite().cache(PERSON_CACHE_NAME);

        // Create query to get names of all employees.
        SqlFieldsQuery qry = new SqlFieldsQuery("select concat(firstName, ' ', lastName) from Person");

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        Collection<List<?>> res = cache.query(qry).getAll();

        // Print names.
        print("Names of all employees:", res);
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void sqlFieldsQueryWithJoin() {
        IgniteCache<?, ?> cache = Ignition.ignite().cache(PERSON_CACHE_NAME);

        // Create query to get names of all employees.
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "select concat(p.firstName, ' ', p.lastName), o.name " +
            "from Person p, \"" + ORG_CACHE_NAME + "\".Organization o " +
            "where p.orgId = o.id");

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        Collection<List<?>> res = cache.query(qry).getAll();

        // Print persons' names and organizations' names.
        printInline("Names of all employees and organizations they belong to (SQL join):", res);
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void aggregateQuery() {
        IgniteCache<?, ?> cache = Ignition.ignite().cache(PERSON_CACHE_NAME);

        // Create query to get sum of salaries and number of summed rows.
        SqlFieldsQuery qry = new SqlFieldsQuery("select sum(salary), count(salary) from Person");

        // Execute query to get collection of rows.
        Collection<List<?>> res = cache.query(qry).getAll();

        double sum = 0;
        long cnt = 0;

        for (List<?> row : res) {
            // Skip results from nodes without data.
            if (row.get(0) != null) {
                sum += (Double)row.get(0);
                cnt += (Long)row.get(1);
            }
        }

        // Print persons' names and organizations' names.
        print("Average employee salary (aggregation query): " + (cnt > 0 ? (sum / cnt) : "n/a"));
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     */
    private static void groupByQuery() {
        IgniteCache<?, ?> cache = Ignition.ignite().cache(PERSON_CACHE_NAME);

        // Create query to get salary averages grouped by organization name.
        // We don't need to perform any extra manual steps here, because
        // Person data is colocated based on organization IDs.
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "select avg(p.salary), o.name " +
            "from Person p, \"" + ORG_CACHE_NAME + "\".Organization o " +
            "where p.orgId = o.Id " +
            "group by o.name " +
            "having avg(p.salary) > ?");

        // Execute query to get collection of rows.
        printInline("Average salaries per Organization (group-by query): ", cache.query(qry.setArgs(500)).getAll());
    }

    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     */
    private static void textQuery() {
        IgniteCache<Long, Person> cache = Ignition.ignite().cache(PERSON_CACHE_NAME);

        //  Query for all people with "Master Degree" in their resumes.
        TextQuery<Long, Person> masters = new TextQuery<>(Person.class, "Master");

        // Query for all people with "Bachelor Degree"in their resumes.
        TextQuery<Long, Person> bachelors = new TextQuery<>(Person.class, "Bachelor");

        print("Following people have 'Master Degree' in their resumes: ", cache.query(masters).getAll());
        print("Following people have 'Bachelor Degree' in their resumes: ", cache.query(bachelors).getAll());
    }

    /**
     * Populate cache with test data.
     *
     * @throws InterruptedException In case of error.
     */
    private static void initialize() throws InterruptedException {
        IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE_NAME);
        IgniteCache<PersonKey, Person> personCache = Ignition.ignite().cache(PERSON_CACHE_NAME);

        // Clear caches before start.
        personCache.clear();
        orgCache.clear();

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");
        Person p5 = new Person(org2, "Helen", "Gerrard", 800, "Helen has Bachelor Degree.");
        Person p6 = new Person(org2, "Darth", "Vader", 1250, "Dart has Bachelor Degree.");

        orgCache.put(org1.getId(), org1);
        orgCache.put(org2.getId(), org2);

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        personCache.put(p1.key(), p1);
        personCache.put(p2.key(), p2);
        personCache.put(p3.key(), p3);
        personCache.put(p4.key(), p4);
        personCache.put(p5.key(), p5);
        personCache.put(p6.key(), p6);

        // Wait 1 second to be sure that all nodes processed put requests.
        Thread.sleep(1000);
    }

    /**
     * @param msg Message.
     * @param col Query results.
     */
    private static void printInline(String msg, Iterable<?> col) {
        if (msg != null)
            System.out.println(">>> " + msg);

        printInline(col);
    }

    /**
     * Prints collection of objects to standard out.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        if (msg != null)
            System.out.println(">>> " + msg);

        print(col);
    }

    /**
     * Prints collection items.
     *
     * @param col Collection.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col) {
            if (next instanceof Iterable)
                print((Iterable<?>)next);
            else
                System.out.println(">>>     " + next);
        }
    }

    /**
     * Prints collection items.
     *
     * @param col Collection.
     */
    private static void printInline(Iterable<?> col) {
        for (Object next : col)
            System.out.println(">>>     " + next);
    }

    /**
     * Prints out given object to standard out.
     *
     * @param o Object to print.
     */
    private static void print(Object o) {
        System.out.println(">>> " + o);
    }
}
