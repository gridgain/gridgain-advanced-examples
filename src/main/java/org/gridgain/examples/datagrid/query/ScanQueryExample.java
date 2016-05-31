package org.gridgain.examples.datagrid.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.gridgain.examples.ExampleNodeStartup;
import org.gridgain.examples.model.Organization;
import org.gridgain.examples.model.Person;
import org.gridgain.examples.model.PersonKey;

/**
 * This example shows how to execute a scan query on a node that owns a particular partition.
 *
 * started with special configuration file which enables P2P class loading: {@code 'ignite.{sh|bat}
 * ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'} or {@link ExampleNodeStartup} can be used.
 */
public class ScanQueryExample {
    /** Replicated cache name (to store organizations). */
    private static final String ORG_CACHE_NAME = ScanQueryExample.class.getSimpleName() + "-organizations";

    /** Partitioned cache name (to store employees). */
    private static final String PERSON_CACHE_NAME = ScanQueryExample.class.getSimpleName() + "-persons";

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

                // Create query to get names and salary value of all the persons.
                SqlFieldsQuery qry = new SqlFieldsQuery("select firstName, lastName, salary from Person");

                // Execute query to get collection of rows.
                Collection<List<?>> res = personCache.query(qry).getAll();

                for (List row : res)
                    System.out.println("Person [name=" + ((String)row.get(0) + ' ' + row.get(1)) + ", salary=" +
                        row.get(2) + ']');

                // Getting nodes to owned partitions mapping.
                Map<UUID, List<Integer>> nodesToPart = personCachePartitionsByNode(ignite);

                // Sending closure to all the nodes that ownes a partition of person cache.
                IgniteCompute compute = ignite.compute(ignite.cluster().forNodeIds(nodesToPart.keySet()));

                System.out.println();
                System.out.println("Sending salary increasing closure.");

                compute.broadcast(new SalaryIncreaseClosure(nodesToPart));

                System.out.println("Salary increase completed");

                // Execute query to get collection of rows.
                res = personCache.query(qry).getAll();

                System.out.println();

                for (List row : res)
                    System.out.println("Person [name=" + ((String)row.get(0) + ' ' + row.get(1)) + ", salary=" +
                        row.get(2) + ']');
            }
        }
    }

    /**
     * Building a map that contains mapping of node ID to a list of partitions stored on the node.
     *
     * @param ignite Ignite instance.
     * @return Node to partitions map.
     */
    private static Map<UUID, List<Integer>> personCachePartitionsByNode(Ignite ignite) {
        // Getting affinity for person cache.
        Affinity affinity = ignite.affinity(PERSON_CACHE_NAME);

        // Building a list of all partitions numbers.
        List<Integer> allPartitions = new ArrayList<>(affinity.partitions());

        for (int i = 0; i < affinity.partitions(); i++)
            allPartitions.add(i);

        // Getting partition to node mapping.
        Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(allPartitions);

        // Building node to partitions mapping.
        Map<UUID, List<Integer>> nodesToPart = new HashMap<>();

        for (Map.Entry<Integer, ClusterNode> entry : partPerNodes.entrySet()) {
            List<Integer> nodeParts = nodesToPart.get(entry.getValue().id());

            if (nodeParts == null) {
                nodeParts = new ArrayList<>();
                nodesToPart.put(entry.getValue().id(), nodeParts);
            }

            nodeParts.add(entry.getKey());
        }

        return nodesToPart;
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

    /**
     * Compute closure that iterates over every partition owned by a node and increases a salary for every person
     * located in a partition.
     */
    private static class SalaryIncreaseClosure implements IgniteCallable<Object> {
        /** */
        private Map<UUID, List<Integer>> nodesToPart;

        /** */
        @IgniteInstanceResource
        private Ignite node;

        /** */
        private IgniteCache<PersonKey, Person> cache;

        /**
         * Constructor.
         *
         * @param nodesToPart Nodes to partitions mapping.
         */
        public SalaryIncreaseClosure(Map<UUID, List<Integer>> nodesToPart) {
            this.nodesToPart = nodesToPart;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            cache = node.cache(PERSON_CACHE_NAME);

            // Getting a list of the partitions owned by this node.
            List<Integer> myPartitions = nodesToPart.get(node.cluster().localNode().id());

            // Iterating over every partition and increasing salary for every person.
            for (Integer part : myPartitions) {
                ScanQuery scanQuery = new ScanQuery();

                scanQuery.setPartition(part);

                // Execute the query.
                Iterator<Cache.Entry<PersonKey, Person>> iterator = node.cache(PERSON_CACHE_NAME).
                    query(scanQuery).iterator();

                while (iterator.hasNext()) {
                    PersonKey key = iterator.next().getKey();

                    // Performing update in a transaction.
                    try (Transaction tr = node.transactions().txStart(
                        TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {

                        // Locking the entry.
                        Person person = cache.get(key);

                        person.setSalary(person.getSalary() + person.getSalary() / 10);

                        cache.put(key, person);

                        tr.commit();

                        System.out.println("Increased salary for person: " + person);
                    }
                }
            }

            return null;
        }

    }
}
