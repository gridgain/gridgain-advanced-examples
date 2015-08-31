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

package org.gridgain.examples.localstore;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.store.local.*;

import javax.cache.configuration.*;

/**
 *
 */
public class LocalRecoverableStoreExample {
    /**
     * @param args Arguments (none required).
     */
    public static void main(String[] args) {
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Local store example started.");

            CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

            ccfg.setCacheStoreFactory(
                new Factory<CacheStore<Integer, Integer>>() {
                    @SuppressWarnings("unchecked")
                    @Override public CacheStore<Integer, Integer> create() {
                        return new CacheFileLocalStore();
                    }
                })
                .setReadThrough(true)
                .setWriteThrough(true);

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(ccfg);

            System.out.println("Putting values to cache...");

            // Put section.
            for (int i = 0; i < 20; i++)
                cache.put(i, i);

//            cacheGet(cache);

//            cachePeek(
//                ignite,
//                cache);
        }
    }

    /**
     * @param cache cache.
     */
    private static void cacheGet(IgniteCache<Integer, Integer> cache) {
        System.out.println("Getting values from cache...");

        for (int i = 0; i < 20; i++)
            System.out.println("Value [key=" + i + ", val=" + cache.get(i) + ']');
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     */
    private static void cachePeek(Ignite ignite, IgniteCache<Integer, Integer> cache) {
        ignite.compute(ignite.cluster().forServers()).broadcast(
            new IgniteRunnable() {
                @IgniteInstanceResource
                private transient Ignite ignite;

                @Override public void run() {
                    System.out.println("Peeking values from cache...");

                    IgniteCache<Object, Object> cache = ignite.cache(null);

                    for (int i = 0; i < 20; i++)
                        System.out.println("Peeked value [key=" + i + ", val=" + cache.localPeek(i) + ']');
                }
            }
        );

        cache.loadCache(
            new IgniteBiPredicate<Integer, Integer>() {
                @Override public boolean apply(
                    Integer key,
                    Integer val
                ) {
                    System.out.println("Loading [key=" + key + ", val=" + val + ']');

                    return true;
                }
            });

        ignite.compute(ignite.cluster().forServers()).broadcast(
            new IgniteRunnable() {
                @IgniteInstanceResource
                private transient Ignite ignite;

                @Override public void run() {
                    System.out.println("Peeking values from cache...");

                    IgniteCache<Object, Object> cache = ignite.cache(null);

                    for (int i = 0; i < 20; i++)
                        System.out.println("Peeked value [key=" + i + ", val=" + cache.localPeek(i) + ']');
                }
            }
        );

        System.out.println();
        System.out.println(">>> Check server nodes output.");
    }
}
