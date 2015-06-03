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

package org.gridgain.examples.datagrid.affinity;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * This example demonstrates the simplest code that populates the distributed cache
 * and co-locates simple closure execution with each key. The goal of this particular
 * example is to provide the simplest code example of this logic.
 * <p>
 * Remote nodes should always be started like this:
 * {@code 'ggstart.{sh|bat} config/example-ignite.xml'}.
 */
public final class CacheAffinityExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheAffinityExample.class.getSimpleName();

    /** Number of keys. */
    private static final int KEY_CNT = 20;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache affinity example started.");

            try (IgniteCache<Integer, String> cache = ignite.createCache(CACHE_NAME)) {
                // Clear caches before running example.
                cache.clear();

                for (int i = 0; i < KEY_CNT; i++)
                    cache.put(i, Integer.toString(i));

                // Co-locates jobs with data using GridCompute.affinityRun(...) method.
                visitUsingAffinityRun();

                // Co-locates jobs with data using Grid.mapKeysToNodes(...) method.
                visitUsingMapKeysToNodes();
            }
        }
    }

    /**
     * Collocates jobs with keys they need to work on using
     * {@link IgniteCompute#affinityRun(String, Object, IgniteRunnable)} method.
     */
    private static void visitUsingAffinityRun() {
        Ignite ignite = Ignition.ignite();

        final IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++) {
            final int key = i;

            // This runnable will execute on the remote node where
            // data with the given key is located. Since it will be co-located
            // we can use local 'peek' operation safely.
            ignite.compute().affinityRun(CACHE_NAME, key, new IgniteRunnable() {
                @Override public void run() {
                    // Peek is a local memory lookup, however, value should never be 'null'
                    // as we are co-located with node that has a given key.
                    System.out.println("Co-located using affinityRun [key= " + key +
                        ", value=" + cache.localPeek(key) + ']');
                }
            });
        }
    }

    /**
     * Collocates jobs with keys they need to work on using {@link IgniteCluster#mapKeysToNodes(String, Collection)}
     * method. The difference from {@code affinityRun(...)} method is that here we process multiple keys
     * in a single job.
     */
    private static void visitUsingMapKeysToNodes() {
        final Ignite ignite = Ignition.ignite();

        Collection<Integer> keys = new ArrayList<>(KEY_CNT);

        for (int i = 0; i < KEY_CNT; i++)
            keys.add(i);

        // Map all keys to nodes.
        Map<ClusterNode, Collection<Integer>> mappings = ignite.cluster().mapKeysToNodes(CACHE_NAME, keys);

        for (Map.Entry<ClusterNode, Collection<Integer>> mapping : mappings.entrySet()) {
            ClusterNode node = mapping.getKey();

            final Collection<Integer> mappedKeys = mapping.getValue();

            if (node != null) {
                // Create cluster group with one node.
                ClusterGroup grp = ignite.cluster().forNode(node);

                // Bring computations to the nodes where the data resides (i.e. collocation).
                ignite.compute(grp).run(new IgniteRunnable() {
                    @Override public void run() {
                        IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

                        // Peek is a local memory lookup, however, value should never be 'null'
                        // as we are co-located with node that has a given key.
                        for (Integer key : mappedKeys)
                            System.out.println("Co-located using mapKeysToNodes [key= " + key +
                                ", value=" + cache.localPeek(key) + ']');
                    }
                });
            }
        }
    }
}
