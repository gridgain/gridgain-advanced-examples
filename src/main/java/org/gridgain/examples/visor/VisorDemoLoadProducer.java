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

package org.gridgain.examples.visor;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.examples.*;
import org.gridgain.examples.model.*;

import java.util.concurrent.*;

/**
 * This sample application can be used to produce sample load for
 * Visor GUI demonstration and testing purposes.
 * <p>
 * Start 1 or more remote nodes with {@link ExampleNodeStartup} or
 * {@code 'ggstart.[sh|bat] ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}.
 * Then start Visor GUI with GRIDGAIN_HOME/bin/ggvisorui.{sh | bat} and
 * connect to the started topology using external connect (with default settings)
 * or external connect (with 'ADVANCED-EXAMPLES-DIR/config/example-ignite.xml' config).
 * Then start the example. Visor GUI will show various activities happening in the topology and
 * will allow to run SQL queries with corresponding tab.
 */
@SuppressWarnings("InfiniteLoopStatement")
public class VisorDemoLoadProducer {
    /**
     * @param args Arguments (none required).
     */
    public static void main(String[] args) {
        Ignite ignite = Ignition.start("config/example-ignite.xml");

        startShortTaskProducer(ignite);
        startLongTaskProducer(ignite);

        populateSqlData(ignite);

        startTxCacheLoader(ignite);
        startAtomicCacheLoader(ignite);
    }

    /**
     * @param ignite Ignite.
     */
    private static void startShortTaskProducer(final Ignite ignite) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    while (true) {
                        try {
                            ignite.compute().withName("ShortRunningTask").broadcast(
                                new IgniteRunnable() {
                                    @Override public void run() {
                                        if (ThreadLocalRandom.current().nextDouble() < 0.02)
                                            throw new RuntimeException("Some exception happens with 2% chance.");

                                        System.out.println("ShortRunningTask Runnable finished normally.");
                                    }
                                });
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        sleep(1000);
                    }
                }
            }).start();
    }

    /**
     * @param ignite Ignite.
     */
    private static void startLongTaskProducer(final Ignite ignite) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    while (true) {
                        try {
                            ignite.compute().withName("LongRunningTask").broadcast(
                                new IgniteRunnable() {
                                    @Override public void run() {
                                        try {
                                            Thread.sleep(
                                                ThreadLocalRandom.current().nextInt(
                                                    8000,
                                                    10000));
                                        }
                                        catch (InterruptedException e) {
                                            System.out.println("Sleep interrupted: " + e);

                                            return;
                                        }

                                        System.out.println("LongRunningTask Runnable finished normally.");
                                    }
                                });
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
    }

    /**
     * @param ignite Ignite.
     */
    private static void populateSqlData(Ignite ignite) {
        CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>("organizations");

        orgCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

        CacheConfiguration<PersonKey, Person> personCacheCfg = new CacheConfiguration<>("persons");

        personCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        personCacheCfg.setIndexedTypes(PersonKey.class, Person.class);

        try (
            IgniteCache<Long, Organization> orgCache = ignite.getOrCreateCache(orgCacheCfg);
            IgniteCache<PersonKey, Person> personCache = ignite.getOrCreateCache(personCacheCfg)
        ) {
            // Organizations.
            Organization org1 = new Organization("ApacheIgnite");
            Organization org2 = new Organization("Other");

            orgCache.put(org1.getId(), org1);
            orgCache.put(org2.getId(), org2);

            // People.
            Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
            Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
            Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
            Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

            // Note that in this example we use custom affinity key for Person objects
            // to ensure that all persons are collocated with their organizations.
            personCache.put(p1.key(), p1);
            personCache.put(p2.key(), p2);
            personCache.put(p3.key(), p3);
            personCache.put(p4.key(), p4);
        }
    }

    private static void startTxCacheLoader(final Ignite ignite) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    CacheConfiguration<Long, Long> ccfg = new CacheConfiguration<>("tx-cache");

                    ccfg.setCacheMode(CacheMode.PARTITIONED);
                    ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

                    IgniteCache<Long, Long> cache = ignite.getOrCreateCache(ccfg);

                    while (true) {
                        try (Transaction tx = ignite.transactions().txStart()) {
                            ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            switch (rnd.nextInt(3)) {
                                case 0:
                                    cache.put(rnd.nextLong(10000), rnd.nextLong(10000));

                                    break;

                                case 1:
                                    cache.remove(rnd.nextLong(10000));

                                    break;

                                case 2:
                                    cache.get(rnd.nextLong(10000));

                                    break;
                            }

                            if (rnd.nextDouble() < 0.1)
                                tx.rollback(); // TX gets rolled back with 10% chance.

                            sleep(50);
                        }
                    }
                }
            }
        ).start();
    }

    /**
     * @param ignite Ignite.
     */
    private static void startAtomicCacheLoader(final Ignite ignite) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    CacheConfiguration<Long, Long> ccfg = new CacheConfiguration<>("atomic-cache");

                    ccfg.setCacheMode(CacheMode.PARTITIONED);

                    IgniteCache<Long, Long> cache = ignite.getOrCreateCache(ccfg);

                    while (true) {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        switch (rnd.nextInt(3)) {
                            case 0:
                                cache.put(rnd.nextLong(10000), rnd.nextLong(10000));

                                break;

                            case 1:
                                cache.remove(rnd.nextLong(10000));

                                break;

                            case 2:
                                cache.get(rnd.nextLong(10000));

                                break;
                        }

                        sleep(50);
                    }
                }
            }
        ).start();
    }

    /**
     *
     */
    private static void sleep(long duration) {
        try {
            Thread.sleep(duration);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
