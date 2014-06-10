/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.query;

import org.gridgain.examples.datagrid.model.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;

import java.util.*;

/**
 * This example shows SQL query usage.
 */
public class SqlQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache query example started.");

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

            print("Cache query example finished.");
        }
    }

    /**
     * Example for SQL queries based on salary ranges.
     *
     * @throws GridException In case of error.
     */
    private static void sqlQuery() throws GridException {
        GridCache<GridCacheAffinityKey<UUID>, Person> cache = GridGain.grid().cache(CACHE_NAME);

        // Create query which selects salaries based on range.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> qry =
            cache.queries().createSqlQuery(Person.class, "salary > ? and salary <= ?");

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000: ", qry.execute(0, 1000).get());

        print("People with salaries between 1000 and 2000: ", qry.execute(1000, 2000).get());

        print("People with salaries greater than 2000: ", qry.execute(2000, Integer.MAX_VALUE).get());
    }

    /**
     * Example for SQL queries based on all employees working for a specific organization.
     *
     * @throws GridException In case of error.
     */
    private static void sqlQueryWithJoin() throws GridException {
        GridCache<GridCacheAffinityKey<UUID>, Person> cache = GridGain.grid().cache(CACHE_NAME);

        // Create query which joins on 2 types to select people for a specific organization.
        GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> qry =
            cache.queries().createSqlQuery(Person.class, "from Person, Organization " +
                "where Person.orgId = Organization.id " +
                "and lower(Organization.name) = lower(?)");

        // Execute queries for find employees for different organizations.
        print("Following people are 'GridGain' employees: ", qry.execute("GridGain").get());
        print("Following people are 'Other' employees: ", qry.execute("Other").get());
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     *
     * @throws GridException In case of error.
     */
    private static void sqlFieldsQuery() throws GridException {
        GridCache<?, ?> cache = GridGain.grid().cache(CACHE_NAME);

        // Create query to get names of all employees.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select concat(firstName, ' ', lastName) from Person");

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        Collection<List<?>> res = qry1.execute().get();

        // Print names.
        print("Names of all employees:", res);
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     *
     * @throws GridException In case of error.
     */
    private static void sqlFieldsQueryWithJoin() throws GridException {
        GridCache<?, ?> cache = GridGain.grid().cache(CACHE_NAME);

        // Create query to get names of all employees.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select concat(firstName, ' ', lastName), Organization.name from Person, Organization where " +
                "Person.orgId = Organization.id");

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        Collection<List<?>> res = qry1.execute().get();

        // Print persons' names and organizations' names.
        print("Names of all employees and organizations they belong to:", res);
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     *
     * @throws GridException In case of error.
     */
    private static void aggregateQuery() throws GridException {
        GridCache<?, ?> cache = GridGain.grid().cache(CACHE_NAME);

        // Create query to get sum of salaries and number of summed rows.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select sum(salary), count(salary) from Person");

        // Execute query to get collection of rows.
        Collection<List<?>> res = qry1.execute().get();

        double sum = 0;
        long cnt = 0;

        for (List<?> row : res) {
            sum += (Double)row.get(0);
            cnt += (Long)row.get(1);
        }

        // Print persons' names and organizations' names.
        print("Average employee salary: " + sum / cnt);
    }

    /**
     * Populate cache with test data.
     *
     * @throws GridException In case of error.
     * @throws InterruptedException In case of error.
     */
    private static void initialize() throws GridException, InterruptedException {
        GridCache<?, ?> cache = GridGain.grid().cache(CACHE_NAME);

        // Organization projection.
        GridCacheProjection<Long, Organization> orgCache = cache.projection(Long.class, Organization.class);

        // Person projection.
        GridCacheProjection<GridCacheAffinityKey<Long>, Person> personCache =
            cache.projection(GridCacheAffinityKey.class, Person.class);

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        orgCache.put(org1.getId(), org1);
        orgCache.put(org2.getId(), org2);

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        personCache.put(p1.key(), p1);
        personCache.put(p2.key(), p2);
        personCache.put(p3.key(), p3);
        personCache.put(p4.key(), p4);

        // Wait 1 second to be sure that all nodes processed put requests.
        Thread.sleep(1000);
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
     * Prints out given object to standard out.
     *
     * @param o Object to print.
     */
    private static void print(Object o) {
        System.out.println(">>> " + o);
    }
}
