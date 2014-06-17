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

package org.gridgain.examples.datagrid.entryprocessor;

import org.gridgain.examples.datagrid.*;
import org.gridgain.examples.datagrid.model.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;

import java.util.*;
import java.util.Map.*;

/**
 * This example shows how to send entry manipulation logic to the remote nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}
 * or {@link CacheExampleNodeStartup} can be used.
 */
public class EntryProcessorExample {
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
            // Populate cache with data.
            initialize();

            // Output employees of other organization.
            GridCache<Long, Person> cache = GridGain.grid().cache(PARTITIONED_CACHE_NAME);

            // Create query which joins on 2 types to select people for a specific organization.
            GridCacheQuery<Map.Entry<Long, Person>> qry =
                cache.queries().createSqlQuery(Person.class,
                    "from \"partitioned\".Person, \"replicated\".Organization " +
                        "where Person.orgId = Organization.id " +
                        "and lower(Organization.name) = lower(?)").enableDedup(true);

            System.out.println();
            System.out.println("Initial salaries:");

            // Execute query for find employees for organization.
            for (Entry<Long, Person> p : qry.execute("Other").get())
                System.out.println("Person: " + p);

            // Increase salary for Other's company employees by 10%.
            // 1. Execute query to get the list of Employee IDs.
            // Create query to get IDs of all employees.
            GridCacheQuery<List<?>> qry1 = cache.queries().createSqlFieldsQuery(
                "select Person.id from \"partitioned\".Person, " +
                    "\"replicated\".Organization where Person.orgId = Organization.id " +
                    "and lower(Organization.name) = lower(?)").enableDedup(true);

            // Execute query to get collection of rows. In this particular
            // case each row will have one element with full name of an employees.
            Collection<List<?>> res = qry1.execute("Other").get();

            Set<Long> ids = new HashSet<>(res.size(), 1.0f);

            for (List<?> l : res)
                ids.add((Long)l.get(0));

            System.out.println();
            System.out.println("Will update salaries for Other organizaion by 10%.");

            g.<Long, Person>cache(PARTITIONED_CACHE_NAME).transformAll(ids, new GridClosure<Person, Person>() {
                @Override public Person apply(Person person) {
                    System.out.println("Transform closure has been called with parameter: " + person);

                    if (person == null)
                        return null;

                    // Need to return new instance.
                    Person res = new Person(person);

                    res.setSalary(res.getSalary() * 1.1);

                    return res;
                }
            });

            System.out.println();
            System.out.println("Salaries after update:");

            // Execute query one more time to print salaries after update.
            for (Entry<Long, Person> p : qry.execute("Other").get())
                System.out.println("Person: " + p);
        }
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
}
