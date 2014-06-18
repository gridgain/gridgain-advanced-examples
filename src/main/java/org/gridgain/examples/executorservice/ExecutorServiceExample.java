/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.executorservice;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * This example shows how to use GridGain as distributed executor service.
 */
public class ExecutorServiceExample {
    /**
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            // Get grid-enabled executor service.
            ExecutorService exec = g.compute().executorService();

            Collection<Future<?>> futs = new ArrayList<>();

            // Iterate through all words in the sentence and create jobs.
            for (final String word : "Print words using runnable".split(" ")) {
                // Execute runnable on some node.
                futs.add(exec.submit(new GridRunnable() {
                    @Override public void run() {
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");
                    }
                }));
            }

            for (Future<?> fut : futs)
                fut.get();
        }
    }
}
