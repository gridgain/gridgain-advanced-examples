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

package org.gridgain.examples.compute.continuation;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.math.*;
import java.util.concurrent.*;

/**
 * This example demonstrates how to use continuation feature of GridGain by
 * performing the distributed recursive calculation of {@code 'Fibonacci'}
 * numbers on the grid. Continuations
 * functionality is exposed via {@link ComputeJobContext#holdcc()} and
 * {@link ComputeJobContext#callcc()} method calls in {@link FibonacciClosure} class.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} config/example-ignite.xml'}.
 */
public final class ComputeFibonacciContinuationExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println("Compute Fibonacci continuation example started.");

            long N = 100;

            long start = System.currentTimeMillis();

            BigInteger fib = ignite.compute().apply(new FibonacciClosure(), N);

            long duration = System.currentTimeMillis() - start;

            System.out.println();
            System.out.println(">>> Finished executing Fibonacci for '" + N + "' in " + duration + " ms.");
            System.out.println(">>> Fibonacci sequence for input number '" + N + "' is '" + fib + "'.");
            System.out.println(">>> If you re-run this example w/o stopping remote nodes - the performance will");
            System.out.println(">>> increase since intermediate results are pre-cache on remote nodes.");
            System.out.println(">>> You should see prints out every recursive Fibonacci execution on grid nodes.");
            System.out.println(">>> Check remote nodes for output.");
        }
    }

    /**
     * Closure to execute.
     */
    private static class FibonacciClosure implements IgniteClosure<Long, BigInteger> {
        /** Future for spawned task. */
        private IgniteFuture<BigInteger> fut1;

        /** Future for spawned task. */
        private IgniteFuture<BigInteger> fut2;

        /** Auto-inject job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Auto-inject grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Nullable @Override public BigInteger apply(Long n) {
            if (fut1 == null || fut2 == null) {
                System.out.println();
                System.out.println(">>> Starting fibonacci execution for number: " + n);

                // Make sure n is not negative.
                n = Math.abs(n);

                if (n <= 2)
                    return n == 0 ? BigInteger.ZERO : BigInteger.ONE;

                // Node-local storage.
                ConcurrentMap<Long, IgniteFuture<BigInteger>> locMap = ignite.cluster().nodeLocalMap();

                // Check if value is cached in node-local-map first.
                fut1 = locMap.get(n - 1);
                fut2 = locMap.get(n - 2);

                // If future is not cached in node-local-map, cache it.
                if (fut1 == null)
                    fut1 = execute(n - 1);

                // If future is not cached in node-local-map, cache it.
                if (fut2 == null)
                    fut2 = execute(n - 2);

                // If futures are not done, then wait asynchronously for the result
                if (!fut1.isDone() || !fut2.isDone()) {
                    IgniteInClosure<IgniteFuture<BigInteger>> lsnr = new IgniteInClosure<IgniteFuture<BigInteger>>() {
                        @Override public void apply(IgniteFuture<BigInteger> f) {
                            // If both futures are done, resume the continuation.
                            if (fut1.isDone() && fut2.isDone())
                                // CONTINUATION:
                                // =============
                                // Resume suspended job execution.
                                jobCtx.callcc();
                        }
                    };

                    // CONTINUATION:
                    // =============
                    // Hold (suspend) job execution.
                    // It will be resumed in listener above via 'callcc()' call
                    // once both futures are done.
                    jobCtx.holdcc();

                    // Attach the same listener to both futures.
                    fut1.listen(lsnr);
                    fut2.listen(lsnr);

                    return null;
                }
            }

            assert fut1.isDone() && fut2.isDone();

            // Return cached results.
            return fut1.get().add(fut2.get());
        }

        private IgniteFuture<BigInteger> execute(long n) {
            // Node-local storage.
            ConcurrentMap<Long, IgniteFuture<BigInteger>> locMap = ignite.cluster().nodeLocalMap();

            // Asynchronous compute interface.
            IgniteCompute compute = ignite.compute().withAsync();

            IgniteFuture<BigInteger> fut = locMap.get(n - 1);

            if (fut == null) {
                compute.apply(new FibonacciClosure(), n - 1);

                fut = compute.future();

                IgniteFuture<BigInteger> old = locMap.putIfAbsent(n - 1, fut);

                if (old != null)
                    fut = old;
            }

            return fut;
        }
    }
}
