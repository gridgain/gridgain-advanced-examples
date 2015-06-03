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
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;

import javax.cache.*;
import javax.cache.event.*;

/**
 * This examples demonstrates continuous query API.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}.
 */
public class ContinuousQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = ContinuousQueryExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache continuous query example started.");

            try (IgniteCache<Integer, String> cache = ignite.createCache(CACHE_NAME)) {
                int keyCnt = 20;

                for (int i = 0; i < keyCnt; i++)
                    cache.put(i, Integer.toString(i));

                // Create new continuous query.
                ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

                // This filter will be evaluated remotely on all nodes
                // Entry that pass this filter will be sent to the caller.
                qry.setRemoteFilter(new CacheEntryEventSerializableFilter<Integer, String>() {
                    @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> event) {
                        return event.getKey() > 15;
                    }
                });

                // Callback that is called locally when update notifications are received.
                qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
                    @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> events) {
                        for (CacheEntryEvent<? extends Integer, ? extends String> e : events)
                            System.out.println("Queried entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                    }
                });

                qry.setInitialQuery(new ScanQuery<Integer, String>());

                // Execute query.
                try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                    for (Cache.Entry<Integer, String> e : cur)
                        System.out.println("Iterated entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');

                    // Add a few more keys and watch more query notifications.
                    for (int i = keyCnt; i < keyCnt + 5; i++)
                        cache.put(i, Integer.toString(i));

                    // Wait for a while while callback is notified about remaining puts.
                    Thread.sleep(2000);
                }
            }
        }
    }
}
