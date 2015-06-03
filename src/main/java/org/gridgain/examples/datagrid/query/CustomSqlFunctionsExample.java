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

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;

import java.util.*;

/**
 * This example shows how to implement and configure custom SQL functions.
 * <p>
 * This example is intended for custom SQL functions demonstration and is not supposed to run with remote nodes,
 * however, it can be launched in cluster. In order to do that make sure all nodes in topology have
 * {@link Functions} on class path and are configured in the same way.
 */
public class CustomSqlFunctionsExample {
    /** */
    private static final String CACHE_NAME = CustomSqlFunctionsExample.class.getSimpleName();

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            CacheConfiguration<Integer, Integer> cc = new CacheConfiguration<>(CACHE_NAME);

            cc.setIndexedTypes(Integer.class, Integer.class);
            cc.setSqlFunctionClasses(Functions.class);

            try (IgniteCache<Integer, Integer> c = ignite.createCache(cc)) {
                Random r = new Random();

                for (int i = 0; i < 10; i++)
                    c.put(r.nextInt(1000), r.nextInt(1000));

                SqlFieldsQuery q = new SqlFieldsQuery("select _val, square(_val) from Integer");

                for (List<?> row : c.query(q))
                    System.out.println(row.get(0) + " -> " + row.get(1));
            }
        }
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
        @QuerySqlFunction
        public static int square(int x) {
            System.out.println("Custom function has been called with argument: " + x);

            return x * x;
        }
    }
}
