/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.putget;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.examples.*;

import java.util.*;

/**
 * This example demonstrates very basic operations on cache, such as 'put' and 'get'.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CachePutGetExample {
    /** Cache name. */
    private static final String CACHE_NAME = CachePutGetExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            try (IgniteCache<Integer, String> cache = ignite.createCache(CACHE_NAME)) {
                // Individual puts and gets.
                putGet(ignite);

                // Bulk puts and gets.
                putAllGetAll(ignite);
            }
        }
    }

    /**
     * Execute individual puts and gets.
     */
    private static void putGet(Ignite ignite) throws Exception {
        System.out.println();
        System.out.println(">>> Cache put-get example started.");

        final IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

        final int keyCnt = 20;

        // Put keys in cache.
        for (int i = 0; i < keyCnt; i++)
            cache.put(i, Integer.toString(i));

        System.out.println(">>> Stored values in cache.");

        // Get keys from cache.
        for (int i = 0; i < keyCnt; i++) {
            String val = cache.get(i);

            if (val == null || !val.equals(Integer.toString(i)))
                throw new Exception("Invalid value in cache [key=" + i + ", val=" + val + ']');
        }

        // Cluster group for remote nodes that have cache running.
        ClusterGroup rmts = ignite.cluster().forCacheNodes(CACHE_NAME).forRemotes();

        // If no other cache nodes are started.
        if (rmts.nodes().isEmpty()) {
            System.out.println(">>> Need to start remote nodes to complete example.");

            return;
        }

        // Get and print out values on all remote nodes.
        ignite.compute(rmts).broadcast(new IgniteRunnable() {
            @Override public void run() {
                for (int i = 0; i < keyCnt; i++)
                    System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');
            }
        });
    }

    /**
     * Execute bulk {@code putAll(...)} and {@code getAll(...)} operations.
     */
    private static void putAllGetAll(Ignite ignite) {
        System.out.println();
        System.out.println(">>> Starting putAll-getAll example.");

        final IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

        final int keyCnt = 20;

        // Create batch.
        Map<Integer, String> batch = new HashMap<>(keyCnt);

        for (int i = 0; i < keyCnt; i++)
            batch.put(i, "bulk-" + Integer.toString(i));

        // Bulk-store entries in cache.
        cache.putAll(batch);

        System.out.println(">>> Bulk-stored values in cache.");

        // Bulk-get entries from cache.
        Map<Integer, String> vals = cache.getAll(batch.keySet());

        for (Map.Entry<Integer, String> e : vals.entrySet())
            System.out.println("Got entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');

        // Cluster group for remote nodes that have cache running.
        ClusterGroup rmts = ignite.cluster().forCacheNodes(CACHE_NAME).forRemotes();

        // If no other cache nodes are started.
        if (rmts.nodes().isEmpty()) {
            System.out.println(">>> Need to start remote nodes to complete example.");

            return;
        }

        final Set<Integer> keys = new HashSet<>(batch.keySet());

        // Get values from all remote cache nodes.
        Collection<Map<Integer, String>> retMaps = ignite.compute(rmts).broadcast(new IgniteCallable<Map<Integer,
                                                                                      String>>() {
                @Override public Map<Integer, String> call() {
                    Map<Integer, String> vals = cache.getAll(keys);

                    for (Map.Entry<Integer, String> e : vals.entrySet())
                        System.out.println("Got entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');

                    return vals;
                }
            }
        );

        System.out.println(">>> Got all entries from all remote nodes.");

        // Since we get the same keys on all nodes, values should be equal to the initial batch.
        for (Map<Integer, String> map : retMaps)
            assert map.equals(batch);
    }
}
