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

package org.gridgain.examples.datagrid.interceptor;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.jetbrains.annotations.*;

import javax.cache.*;

/**
 * This example shows how to configure and use cache interceptor to intercept certain
 * cache operations and to affect the returning values or cancel inappropriate updates.
 * <p>
 * This example is intended for interceptor demonstration and is not supposed to run with remote nodes,
 * however, it can be launched in cluster. In order to do that make sure all nodes in topology have
 * {@link Interceptor} on class path.
 */
public class CacheInterceptorExample {
    /** */
    private static final String CACHE_NAME = CacheInterceptorExample.class.getSimpleName();

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            CacheConfiguration<Integer, Integer> cc = new CacheConfiguration<>(CACHE_NAME);

            cc.setInterceptor(new Interceptor());

            try (IgniteCache<Integer, Integer> c = ignite.createCache(cc)) {
                Integer val = c.get(1);

                if (val != null)
                    throw new RuntimeException("Unexpected value [expected=null, actual=" + val + ']');

                c.put(1, 300);
                c.put(2, 50);

                // This get should be intercepted and return 100 instead.
                val = c.get(2);

                if (val == null || val != 100)
                    throw new RuntimeException("Unexpected value [expected=100, actual=" + val + ']');

                // This update should be cancelled by the interceptor.
                c.put(1, 500);

                val = c.get(1);

                if (val == null || val != 300)
                    throw new RuntimeException("Unexpected value [expected=300, actual=" + val + ']');

                // This update should pass the interceptor.
                c.replace(1, 300, 150);

                val = c.get(1);

                if (val == null || val != 150)
                    throw new RuntimeException("Unexpected value [expected=150, actual=" + val + ']');
            }
        }
    }

    /**
     *
     */
    private static class Interceptor extends CacheInterceptorAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Nullable @Override public Integer onGet(Integer key, Integer val) {
            System.out.println("Intercepting onGet [key=" + key + ", val=" + val + ']');

            // Never return anything less than 100.
            // Cast is needed to protect from NPE in case super returns null
            // which cannot be converted to primitive int.
            return val != null && val < 100 ?
                (Integer)100 :
                super.onGet(key, val);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            System.out.println("Intercepting onBeforePut [key=" + entry.getKey() + ", oldVal=" + entry.getValue() +
                ", newVal=" + newVal + ']');

            // Deny update existing values to anything greater than 200.
            // However if old value is null, all new values are accepted.
            return entry.getValue() != null && newVal > 200 ? null : super.onBeforePut(entry, newVal);
        }
    }
}
