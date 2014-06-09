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

package org.gridgain.examples.datagrid.query;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;

import java.util.*;

/**
 * This examples shows usage of spatial indexes.
 */
public class SpatialQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            GridCache<Integer, Entry> c = g.cache(CACHE_NAME);

            Random rnd = new Random();

            WKTReader r = new WKTReader();

            for (int i = 0; i < 1000; i++) {
                int x = rnd.nextInt(10000);
                int y = rnd.nextInt(10000);

                Geometry geo = r.read("POINT(" + x + " " + y + ")");

                c.put(i, new Entry(geo));
            }

            GridCacheQuery<Map.Entry<Integer, Entry>> q = c.queries().createSqlQuery(Entry.class,
                "coords && ?");

            for (int i = 0; i < 10; i++) {
                Geometry cond = r.read("POLYGON((0 0, 0 " + rnd.nextInt(10000) + ", " +
                    rnd.nextInt(10000) + " " + rnd.nextInt(10000) + ", " +
                    rnd.nextInt(10000) + " 0, 0 0))");

                Collection<Map.Entry<Integer, Entry>> entries = q.execute(cond).get();

                System.out.println("Fetched points [cond=" + cond + ", cnt=" + entries.size() + ']');
            }
        }
    }

    /**
     * Entry with index.
     */
    private static class Entry {
        /** Coordinates. */
        @GridCacheQuerySqlField(index = true)
        private Geometry coords;

        /**
         * @param coords Coordinates.
         */
        private Entry(Geometry coords) {
            this.coords = coords;
        }
    }
}
