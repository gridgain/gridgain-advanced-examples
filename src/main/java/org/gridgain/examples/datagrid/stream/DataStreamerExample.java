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

package org.gridgain.examples.datagrid.stream;

import org.apache.ignite.*;
import org.gridgain.examples.*;

import java.io.*;

/**
 * Demonstrates how cache can be populated with data utilizing {@link IgniteDataStreamer} API.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}
 * or {@link ExampleNodeStartup} can be used.
 */
public class DataStreamerExample {
    /** Cache name. */
    private static final String CACHE_NAME = DataStreamerExample.class.getSimpleName();

    /** Number of entries to load. */
    private static final int ENTRY_COUNT = 500000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws IOException {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache data loader example started.");

            try (IgniteCache<Integer, Integer> cache = ignite.createCache(CACHE_NAME)) {
                try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(CACHE_NAME)) {
                    // Configure loader.
                    streamer.perNodeBufferSize(1024);

                    long start = System.currentTimeMillis();

                    for (int i = 0; i < ENTRY_COUNT; i++) {
                        streamer.addData(i, i);

                        // Print out progress while loading cache.
                        if (i > 0 && i % 10000 == 0) System.out.println("Loaded " + i + " keys.");
                    }

                    long end = System.currentTimeMillis();

                    System.out.println(">>> Loaded " + ENTRY_COUNT + " keys in " + (end - start) + "ms.");
                }

                System.out.println(">>> Hit enter to stop the node.");

                System.in.read();
            }
        }
    }
}
