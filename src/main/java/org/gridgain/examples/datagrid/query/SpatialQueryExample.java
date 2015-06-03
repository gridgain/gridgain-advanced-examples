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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;

import javax.cache.*;
import java.util.*;

/**
 * This examples shows usage of spatial indexes.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}.
 */
public class SpatialQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = SpatialQueryExample.class.getSimpleName();

    /**
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            CacheConfiguration<Integer, Entry> cc = new CacheConfiguration<>(CACHE_NAME);

            cc.setIndexedTypes(Integer.class, Entry.class);

            try (IgniteCache<Integer, Entry> c = ignite.createCache(cc)) {
                Random rnd = new Random();

                WKTReader r = new WKTReader();

                for (int i = 0; i < 1000; i++) {
                    int x = rnd.nextInt(10000);
                    int y = rnd.nextInt(10000);

                    Geometry geo = r.read("POINT(" + x + " " + y + ")");

                    c.put(i, new Entry(geo));
                }

                SqlQuery<Integer, Entry> q = new SqlQuery<>(Entry.class, "coords && ?");

                for (int i = 0; i < 10; i++) {
                    Geometry cond = r.read("POLYGON((0 0, 0 " + rnd.nextInt(10000) + ", " +
                        rnd.nextInt(10000) + " " + rnd.nextInt(10000) + ", " +
                        rnd.nextInt(10000) + " 0, 0 0))");

                    Collection<Cache.Entry<Integer, Entry>> entries = c.query(q.setArgs(cond)).getAll();

                    System.out.println("Fetched points [cond=" + cond + ", cnt=" + entries.size() + ']');
                }
            }
        }
    }

    /**
     * Entry with index.
     */
    private static class Entry {
        /** Coordinates. */
        @QuerySqlField(index = true)
        private Geometry coords;

        /**
         * @param coords Coordinates.
         */
        private Entry(Geometry coords) {
            this.coords = coords;
        }
    }
}
