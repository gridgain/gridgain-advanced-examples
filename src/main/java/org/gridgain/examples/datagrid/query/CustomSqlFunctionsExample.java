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

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.spi.indexing.h2.*;

import java.util.*;

/**
 * This example shows how to implement and configure custom SQL functions.
 * <p>
 * This example is intended for custom SQL functions demonstration and is not supposed to run with remote nodes,
 * however, it can be launched in cluster. In order to do that make sure all nodes in topology have
 * {@link Functions} on class path and are configured in the same way.
 */
public class CustomSqlFunctionsExample {
    /**
     * @param args Args.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start(configuration())) {
            Random r = new Random();

            GridCache<Integer, Integer> c = g.cache(null);

            // Clear caches before running example.
            c.globalClearAll();

            for (int i = 0; i < 10; i++)
                c.put(r.nextInt(1000), r.nextInt(1000));

            GridCacheQuery<List<?>> q = c.queries().createSqlFieldsQuery("select square(_val) from Integer");

            System.out.println();
            System.out.println("Rows count examined with query: " +
                q.enableDedup(true).execute().get().size());
        }
    }

    /**
     * @return Configuration.
     */
    private static GridConfiguration configuration() {
        GridConfiguration c = new GridConfiguration();

        c.setLocalHost("127.0.0.1");
        c.setPeerClassLoadingEnabled(true);

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setQueryIndexEnabled(true);

        c.setCacheConfiguration(cc);

        // Configure indexing SPI...
        GridH2IndexingSpi idxSpi = new GridH2IndexingSpi();

        // ...and set custom function classes.
        idxSpi.setIndexCustomFunctionClasses(Functions.class);
        idxSpi.setDefaultIndexPrimitiveValue(true);

        c.setIndexingSpi(idxSpi);

        return c;
    }

    /**
     * Function definitions.
     */
    public static class Functions {
        /**
         * Function must be a static method.
         *
         * @param x Argument.
         * @return Square of given value.
         */
        @GridCacheQuerySqlFunction
        public static int square(int x) {
            System.out.println("Custom function has been called with argument: " + x);

            return x * x;
        }
    }
}
