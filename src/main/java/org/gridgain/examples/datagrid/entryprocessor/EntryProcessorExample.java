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

package org.gridgain.examples.datagrid.entryprocessor;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.gridgain.examples.*;
import org.gridgain.examples.model.*;

import javax.cache.*;
import javax.cache.processor.*;
import java.util.*;

/**
 * This example shows how to send entry manipulation logic to the remote nodes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}
 * or {@link ExampleNodeStartup} can be used.
 */
public class EntryProcessorExample {
    /** Replicated cache name (to store organizations). */
    private static final String ORG_CACHE_NAME = EntryProcessorExample.class.getSimpleName() + "-organizations";

    /** Partitioned cache name (to store employees). */
    private static final String PERSON_CACHE_NAME = EntryProcessorExample.class.getSimpleName() + "-persons";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
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
                // Populate cache with data.
                initialize();

                // Organization name to query.

                // Create query which joins on 2 types to select people for a specific organization.
                SqlQuery<PersonKey, Person> qry = new SqlQuery<>(Person.class,
                    "from Person, \"" + ORG_CACHE_NAME + "\".Organization " +
                    "where Person.orgId = Organization.id " +
                    "and lower(Organization.name) = lower(?)"
                );

                System.out.println();
                System.out.println("Initial salaries:");

                // Execute query for find employees for organization.
                final String orgName = "GridGain";

                for (Cache.Entry<PersonKey, Person> p : personCache.query(qry.setArgs(orgName)))
                    System.out.println("Person: " + p.getValue());

                // Increase salary for a company employees by 10%.
                // 1. Execute query to get the list of Employee IDs.
                // Create query to get IDs of all employees.
                SqlFieldsQuery qry1 = new SqlFieldsQuery(
                    "select p.id, o.id from Person p, \"" + ORG_CACHE_NAME + "\".Organization o " +
                    "where p.orgId = o.id " +
                    "and lower(o.name) = lower(?)"
                );

                // Execute query to get collection of rows. In this particular
                // case each row will have one element with full name of an employees.
                Collection<List<?>> res = personCache.query(qry1.setArgs(orgName)).getAll();

                Set<PersonKey> ids = new HashSet<>(res.size(), 1.0f);

                for (List<?> l : res)
                    ids.add(new PersonKey((Long)l.get(0), (Long)l.get(1)));

                System.out.println();
                System.out.println("Will update salaries for a given organization by 10%.");

                personCache.invokeAll(ids, new EntryProcessor<PersonKey, Person, Object>() {
                    @Override public Object process(MutableEntry<PersonKey, Person> entry, Object... args) {
                        Person person = entry.getValue();

                        if (person == null)
                            return null;

                        System.out.println("Transform closure has been called for person: " + person.getFirstName());

                        // Need to return new instance.
                        Person res = new Person(person);

                        // Do not use * 1.1 to prevent rounding errors.
                        res.setSalary(res.getSalary() + res.getSalary() / 10);

                        entry.setValue(res);

                        return null;
                    }
                });

                System.out.println();
                System.out.println("Salaries after update:");

                // Execute query one more time to print salaries after update.
                for (Cache.Entry<PersonKey, Person> p : personCache.query(qry.setArgs(orgName)))
                    System.out.println("Person: " + p);
            }
        }
    }

    /**
     * Populate cache with test data.
     *
     * @throws InterruptedException In case of error.
     */
    private static void initialize() throws InterruptedException {
        IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE_NAME);
        IgniteCache<PersonKey, Person> personCache = Ignition.ignite().cache(PERSON_CACHE_NAME);

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
}
