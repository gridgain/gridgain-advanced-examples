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

import org.gridgain.examples.datagrid.*;
import org.gridgain.examples.datagrid.model.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;

import java.util.*;
import java.util.Map.*;

/**
 * This example shows SQL query usage.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}
 * or {@link CacheExampleNodeStartup} can be used.
 */
public class SqlQueryExample {
    /** Partitioned cache name (to store employees). */
    private static final String PARTITIONED_CACHE_NAME = "partitioned";

    /** Replicated cache name (to store organizations). */
    private static final String REPLICATED_CACHE_NAME = "replicated";

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

            // Example for SQL-based querying with custom remote and local reducers
            // to calculate average salary among all employees within a company.
            sqlQueryWithReducers();

            // Example for SQL-based querying with custom remote transformer to make sure
            // that only required data without any overhead is returned to caller.
            sqlQueryWithTransformer();

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

    /**
     * Example for SQL queries based on salary ranges.
     *
     * @throws GridException In case of error.
     */
    private static void sqlQuery() throws GridException {
        GridCache<Long, Person> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        // Create query which selects salaries based on range.
        GridCacheQuery<Entry<Long, Person>> qry = cache.queries().createSqlQuery(
            Person.class,
            "salary > ? and salary <= ?").enableDedup(true);

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
        GridCache<Long, Person> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        // Create query which joins on 2 types to select people for a specific organization.
        GridCacheQuery<Map.Entry<Long, Person>> qry =
            cache.queries().createSqlQuery(Person.class,
                "from \"partitioned\".Person, \"replicated\".Organization " +
                "where Person.orgId = Organization.id " +
                "and lower(Organization.name) = lower(?)").enableDedup(true);

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
        GridCache<?, ?> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        // Create query to get names of all employees.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select concat(firstName, ' ', lastName) from Person").enableDedup(true);

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
        GridCache<?, ?> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        // Create query to get names of all employees.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select concat(firstName, ' ', lastName), Organization.name from \"partitioned\".Person, " +
                "\"replicated\".Organization where Person.orgId = Organization.id").enableDedup(true);

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
        GridCache<?, ?> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        // Create query to get sum of salaries and number of summed rows.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select sum(salary), count(salary) from Person");

        // Execute query to get collection of rows.
        Collection<List<?>> res = qry1.execute().get();

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
        print("Average employee salary: " + (cnt > 0 ? (sum / cnt) : "n/a"));
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     *
     * @throws GridException In case of error.
     */
    private static void groupByQuery() throws GridException {
        GridCache<?, ?> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        // Create query to get sum of salaries and number of summed rows
        // grouping results by organization name.
        GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
            "select sum(salary), count(salary), Organization.name " +
                "from \"partitioned\".Person, \"replicated\".Organization " +
                "where Person.orgId = Organization.Id group by Organization.name");

        // Execute query to get collection of rows.
        Collection<List<?>> res = qry1.execute().get();

        Map<String, GridBiTuple<Double, Long>> map = new HashMap<>();

        for (List<?> row : res) {
            // Skip results from nodes without data.
            if (row.get(0) != null) {
                String orgName = (String)row.get(2);

                GridBiTuple<Double, Long> t = map.get(orgName);

                if (t == null)
                    map.put(orgName, t = new GridBiTuple<>(0.0, 0L));

                t.set1(t.get1() + (Double)row.get(0));
                t.set2(t.get2() + (Long)row.get(1));
            }
        }

        // Print persons' names and organizations' names.
        print("Average employee salary in each organization: ");

        for (Entry<String, GridBiTuple<Double, Long>> e : map.entrySet()) {
            print(e.getKey() + " " +
                (e.getValue().get2() != 0 ?
                    e.getValue().get1() / e.getValue().get2() :
                    "n/a"));
        }
    }

    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     *
     * @throws GridException In case of error.
     */
    private static void textQuery() throws GridException {
        GridCache<Long, Person> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        //  Query for all people with "Master Degree" in their resumes.
        GridCacheQuery<Map.Entry<Long, Person>> masters =
            cache.queries().createFullTextQuery(Person.class, "Master");

        // Query for all people with "Bachelor Degree"in their resumes.
        GridCacheQuery<Map.Entry<Long, Person>> bachelors =
            cache.queries().createFullTextQuery(Person.class, "Bachelor");

        print("Following people have 'Master Degree' in their resumes: ", masters.execute().get());
        print("Following people have 'Bachelor Degree' in their resumes: ", bachelors.execute().get());
    }

    /**
     * Example for SQL queries with custom remote and local reducers to calculate
     * average salary for a specific organization.
     *
     * @throws GridException In case of error.
     */
    private static void sqlQueryWithReducers() throws GridException {
        GridCacheProjection<Long, Person> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        // Calculate average of salary of all persons in GridGain.
        GridCacheQuery<Entry<Long, Person>> qry = cache.queries().createSqlQuery(
            Person.class,
            "from \"partitioned\".Person, \"replicated\".Organization " +
                "where Person.orgId = Organization.id and lower(Organization.name) = lower(?)");

        Collection<GridBiTuple<Double, Integer>> res = qry.execute(
            new GridReducer<Map.Entry<Long, Person>, GridBiTuple<Double, Integer>>() {
                private double sum;

                private int cnt;

                @Override public boolean collect(Map.Entry<Long, Person> e) {
                    sum += e.getValue().getSalary();

                    cnt++;

                    // Continue collecting.
                    return true;
                }

                @Override public GridBiTuple<Double, Integer> reduce() {
                    return new GridBiTuple<>(sum, cnt);
                }
            }, "GridGain").get();

        double sum = 0.0d;
        int cnt = 0;

        for (GridBiTuple<Double, Integer> t : res) {
            sum += t.get1();
            cnt += t.get2();
        }

        double avg = sum / cnt;

        // Calculate average salary for a specific organization.
        print("Average salary for 'GridGain' employees: " + avg);
    }

    /**
     * Example for SQL queries with custom transformer to allow passing
     * only the required set of fields back to caller.
     *
     * @throws GridException In case of error.
     */
    private static void sqlQueryWithTransformer() throws GridException {
        GridCache<Long, Person> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

        // Create query to get names of all employees working for some company.
        GridCacheQuery<Map.Entry<Long, Person>> qry =
            cache.queries().createSqlQuery(Person.class,
                "from \"partitioned\".Person, \"replicated\".Organization " +
                    "where Person.orgId = Organization.id and lower(Organization.name) = lower(?)");

        // Transformer to convert Person objects to String.
        // Since caller only needs employee names, we only
        // send names back.
        GridClosure<Map.Entry<Long, Person>, String> trans =
            new GridClosure<Map.Entry<Long, Person>, String>() {
                @Override public String apply(Map.Entry<Long, Person> e) {
                    return e.getValue().getFirstName() + " " + e.getValue().getLastName();
                }
            };

        // Query all nodes for names of all GridGain employees.
        print("Names of all 'GridGain' employees: " + qry.execute(trans, "GridGain").get());
    }

    /**
     * Populate cache with test data.
     *
     * @throws GridException In case of error.
     * @throws InterruptedException In case of error.
     */
    private static void initialize() throws GridException, InterruptedException {
        // Organization projection.
        GridCacheProjection<Long, Organization> orgCache = GridGain.grid().cache(REPLICATED_CACHE_NAME);

        // Person projection.
        GridCacheProjection<Long, Person> personCache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

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
        personCache.put(p1.getId(), p1);
        personCache.put(p2.getId(), p2);
        personCache.put(p3.getId(), p3);
        personCache.put(p4.getId(), p4);
        personCache.put(p5.getId(), p5);
        personCache.put(p6.getId(), p6);

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
