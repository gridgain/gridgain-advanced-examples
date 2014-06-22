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

package org.gridgain.examples.datagrid.projection;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;

/**
 * This example demonstrates how different cache projections can be used.
 */
public class CacheProjectionExample {
    /** Cache name. */
    public static final String CACHE_NAME = "partitioned";

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            // Get type-unaware cache.
            GridCache<Object, Object> cache = g.cache(CACHE_NAME);

            // Populate cache with Integer values.
            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            // Populate cache with String values.
            for (int i = 10; i < 20; i++)
                cache.put(i, "value-" + i);

            // Get Integer values projection.
            GridCacheProjection<Integer, Integer> intPrj = cache.projection(Integer.class, Integer.class);

            // Check that we will get only Integer values.
            System.out.println(">>> Checking Integer projection.");

            for (int key = 0; key < 20; key++)
                System.out.println("Got value from Integer projection [key=" + key + ", val=" + intPrj.get(key) + ']');

            System.out.println();

            // Get String values projection.
            GridCacheProjection<Integer, String> strPrj = cache.projection(Integer.class, String.class);

            // Check that we will get only String values.
            System.out.println(">>> Checking String projection.");

            for (int key = 0; key < 20; key++)
                System.out.println("Got value from String projection [key=" + key + ", val=" + strPrj.get(key) + ']');

            System.out.println();

            // Create projection with filter based on values.
            GridCacheProjection<Integer, Integer> filterPrj = intPrj.projection(
                new GridBiPredicate<Integer, Integer>() {
                    @Override public boolean apply(Integer key, Integer val) {
                        return val > 4;
                    }
                });

            // Check that we will get only Integer greater than 4.
            System.out.println(">>> Checking filtered projection.");

            for (int key = 0; key < 20; key++)
                System.out.println("Got value from Integer projection [key=" + key + ", val=" + filterPrj.get(key) + ']');
        }
    }
}
