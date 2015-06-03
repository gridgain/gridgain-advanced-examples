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

package org.gridgain.examples.jpa;

import org.apache.ignite.*;
import org.hibernate.*;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.*;
import org.hibernate.service.*;
import org.hibernate.stat.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * This example demonstrates the use of GridGain In-Memory Data Grid as a Hibernate
 * Second-Level cache provider with JPA annotations.
 * <p>
 * The Hibernate Second-Level cache (or "L2 cache" shortly) lets you significantly
 * reduce the number of requests to the underlying SQL database. Because database
 * access is known to be an expansive operation, using L2 cache may improve
 * performance dramatically.
 * <p>
 * This example defines 2 entity classes: {@link Organization} and {@link Employee}, with
 * 1 <-> N relation, and marks them with appropriate annotations for Hibernate
 * object-relational mapping to SQL tables of an underlying H2 in-memory database.
 * The example launches GridGain node in the same JVM and registers it in
 * Hibernate configuration as an L2 cache implementation. It then stores and
 * queries instances of the entity classes to and from the database, having
 * Hibernate SQL output, L2 cache statistics output, and GridGain cache metrics
 * output enabled.
 * <p>
 * When running example, it's easy to notice that when an object is first
 * put into a database, the L2 cache is not used and it's contents is empty.
 * However, when an object is first read from the database, it is immediately
 * stored in L2 cache (which is GridGain In-Memory Data Grid in fact), which can
 * be seen in stats output. Further requests of the same object only read the data
 * from L2 cache and do not hit the database.
 * <p>
 * In this example, the Hibernate query cache is also enabled. Query cache lets you
 * avoid hitting the database in case of repetitive queries with the same parameter
 * values. You may notice that when the example runs the same query repeatedly in
 * loop, only the first query hits the database and the successive requests take the
 * data from L2 cache.
 * <p>
 * Note: this example uses {@link AccessType#READ_ONLY} L2 cache access type, but you
 * can experiment with other access types by modifying the Hibernate configuration file
 * {@code GRIDGAIN_HOME/examples/config/hibernate/example-hibernate-L2-cache.xml}, used by the example.
 * <p>
 * Remote nodes should always be started using {@link JpaHibernateExampleNodeStartup}
 */
public class JpaHibernateExample {
    /** JDBC URL for backing database (an H2 in-memory database is used). */
    private static final String JDBC_URL = "jdbc:h2:mem:example;DB_CLOSE_DELAY=-1";

    /** Path to hibernate configuration file (will be resolved from application {@code CLASSPATH}). */
    private static final String HIBERNATE_CFG = "config/hibernate/example-hibernate-L2-cache.xml";

    /** Entity names for stats output. */
    private static final List<String> ENTITY_NAMES =
        Arrays.asList(Organization.class.getName(), Employee.class.getName(),
            Organization.class.getName() + ".employees", "org.hibernate.cache.internal.StandardQueryCache");

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        // Start the GridGain node, run the example, and stop the node when finished.
        try (Ignite ignite = Ignition.start(JpaHibernateExampleNodeStartup.configuration())) {
            // We use a single session factory, but create a dedicated session
            // for each transaction or query. This way we ensure that L1 cache
            // is not used (L1 cache has per-session scope only).
            System.out.println();
            System.out.println(">>> Hibernate L2 cache example started.");

            URL hibernateCfg = new File(HIBERNATE_CFG).toURI().toURL();

            SessionFactory sesFactory = createHibernateSessionFactory(hibernateCfg);

            System.out.println();
            System.out.println(">>> Creating objects.");

            final long orgId;

            Session ses = sesFactory.openSession();

            try {
                Transaction tx = ses.beginTransaction();

                Organization organization = new Organization("TAX1", "BigBrother Inc.");

                organization.getEmployees().add(new Employee(organization, "Winston", "Smith"));

                ses.save(organization);

                tx.commit();

                // Create an organization object, store it in DB, and save the database-generated
                // object ID. You may try adding more objects in a similar way.
                orgId = organization.getId();
            }
            finally {
                ses.close();
            }

            // Output L2 cache and GridGain cache stats. You may notice that
            // at this point the object is not yet stored in L2 cache, because
            // the read was not yet performed.
            printStats(sesFactory);

            System.out.println();
            System.out.println(">>> Querying object by ID.");

            // Query organization by ID several times. First time we get an L2 cache
            // miss, and the data is queried from DB, but it is then stored
            // in cache and successive queries hit the cache and return
            // immediately, no SQL query is made.
            for (int i = 0; i < 3; i++) {
                ses = sesFactory.openSession();

                try {
                    Transaction tx = ses.beginTransaction();

                    Organization organization = (Organization)ses.get(Organization.class, orgId);

                    System.out.println("User: " + organization);

                    for (Employee employee : organization.getEmployees())
                        System.out.println("\tPost: " + employee);

                    tx.commit();
                }
                finally {
                    ses.close();
                }
            }

            // Output the stats. We should see 1 miss and 2 hits for
            // Organization and Collection object (stored separately in L2 cache).
            // The Employee is loaded with the collection, so it won't imply
            // a miss.
            printStats(sesFactory);

            // Query data with HQL. Should observe the same behaviour as with query-by-id.
            for (int i = 0; i < 3; i++) {
                ses = sesFactory.openSession();

                try {
                    Query qry = ses.createQuery("from Employee e where e.firstName=:firstName");

                    qry.setString("firstName", "Winston");

                    qry.setCacheable(true);

                    List lst = qry.list();

                    for (Object o : lst) {
                        Employee next = (Employee)o;

                        System.out.println("Employee: " + next);
                    }
                }
                finally {
                    ses.close();
                }
            }

            printStats(sesFactory);
        }
    }

    /**
     * Creates a new Hibernate {@link SessionFactory} using a programmatic
     * configuration.
     *
     * @param hibernateCfg Hibernate configuration file.
     * @return New Hibernate {@link SessionFactory}.
     */
    private static SessionFactory createHibernateSessionFactory(URL hibernateCfg) {
        ServiceRegistryBuilder builder = new ServiceRegistryBuilder();

        builder.applySetting("hibernate.connection.url", JDBC_URL);
        builder.applySetting("hibernate.show_sql", true);

        return new Configuration()
            .configure(hibernateCfg)
            .buildSessionFactory(builder.buildServiceRegistry());
    }

    /**
     * Prints Hibernate L2 cache statistics to standard output.
     *
     * @param sesFactory Hibernate {@link SessionFactory}, for which to print
     *                   statistics.
     */
    private static void printStats(SessionFactory sesFactory) {
        System.out.println("=== Hibernate L2 cache statistics ===");

        for (String entityName : ENTITY_NAMES) {
            System.out.println("\tEntity: " + entityName);

            SecondLevelCacheStatistics stats =
                sesFactory.getStatistics().getSecondLevelCacheStatistics(entityName);

            System.out.println("\t\tHits: " + stats.getHitCount());
            System.out.println("\t\tMisses: " + stats.getMissCount());
        }

        System.out.println("=====================================");
    }
}
