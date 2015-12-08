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

package org.gridgain.examples.compute.masterleave;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSessionScope;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

/**
 * Example demonstrates usage of {@link ComputeJobMasterLeaveAware} interface, checkpoints and task sessions with
 * ComputeTaskSessionScope.GLOBAL_SCOPE.
 *
 * The following example is considered to be demonstrating following these steps:
 * - start a remote node using {@link ComputeLeaveAwareNodeStartup};
 * - start this examples and check that the local node (started using this example) and the remote one are executing
 * {@link MasterLeaveAwareJob};
 * - stop this example before jobs are completed;
 * - check logs on the remote node. The logs must contain info saying that the master node left topology and that
 * remote job execution is halted;
 * - start this example one more time;
 * - logs of the remote node must contain info saying that the job continues starting from some point that was saved
 * using checkpoints.
 */
public class ComputeMasterLeaveAwareExample {
    /** */
    private final static String CHECKPOINT_KEY = "checkpoint_key";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // Configuring cache to use for checkpoints.
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("checkpoints");
        cacheCfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(cacheCfg);

        // Configuring checkpoints spi.
        CacheCheckpointSpi checkpointSpi = new CacheCheckpointSpi();

        checkpointSpi.setCacheName(cacheCfg.getName());

        // Overriding default checkpoints SPI
        cfg.setCheckpointSpi(checkpointSpi);

        HashMap<String, Object> attrs = new HashMap<>();
        attrs.put(CHECKPOINT_KEY, "master_node");

        cfg.setUserAttributes(attrs);

        try (Ignite ignite = Ignition.start(cfg)) {
            if (ignite.cluster().nodes().size() < 2)
                throw new RuntimeException("Not enough nodes in the topology to demonstrate the example");

            IgniteCompute compute = ignite.compute();

            String res = compute.execute(new MasterLeaveAwareTask(), 60_000L);

            System.out.println("Example completed: " + res);
        }
    }

    /**
     * Tasks that splits jobs across all available nodes.
     */
    @ComputeTaskSessionFullSupport
    private static class MasterLeaveAwareTask extends ComputeTaskSplitAdapter<Long, String> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int nodesCnt, Long ttl) throws IgniteException {
            ArrayList<MasterLeaveAwareJob> list = new ArrayList(nodesCnt);

            for (int i = 0; i < nodesCnt; i++)
                list.add(new MasterLeaveAwareJob(ttl));

            return list;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String reduce(List<ComputeJobResult> list) throws IgniteException {
            StringBuilder builder = new StringBuilder();

            for (ComputeJobResult result: list)
                builder.append(result.getData()).append(" ");

            return builder.toString();
        }
    }

    private static class MasterLeaveAwareJob implements ComputeJob, ComputeJobMasterLeaveAware {
        /** Counter value. */
        private long cnt;

        /** Job execution time. */
        private long ttl;

        /** Flag indication that the computation must be stopped immediately. */
        private volatile boolean forceStop;

        /** */
        @TaskSessionResource
        ComputeTaskSession session;

        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /**
         * Constructor.
         *
         * @param ttl Job execution duration.
         */
        public MasterLeaveAwareJob(long ttl) {
            this.ttl = ttl;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // Ignored.
        }

        /** {@inheritDoc} */
        @Override public Long execute() throws IgniteException {
            Long oldCnt = session.loadCheckpoint(checkpointKey());

            long startTs = System.currentTimeMillis();

            if (oldCnt != null) {
                cnt = oldCnt;

                System.out.println("Continue calculation. Previously stored value using checkpoints: " + cnt);
            }
            else
                System.out.println("Start calculation from scratch");

            while (!forceStop && (System.currentTimeMillis() - startTs < ttl)) {
                cnt++;

                // Block job execution for 2 second.
                try {
                    Thread.sleep(2000);
                }
                catch (InterruptedException e) {
                    // Ignore
                }

                System.out.println("Cnt: " + cnt);
            }

            if (forceStop) {
                System.err.println("Stopped job execution");

                // Using the global scope to preserve the state across task executions.
                session.saveCheckpoint(checkpointKey(), cnt, ComputeTaskSessionScope.GLOBAL_SCOPE, 0);
            }
            else
                session.removeCheckpoint(checkpointKey());

            return cnt;
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession session) throws IgniteException {
            System.err.println("Master left topology. Stopping the calculation...");

            forceStop = true;
        }

        /**
         * Returns checkpoint key.
         *
         * @return Checkpoint key to use.
         */
        private String checkpointKey() {
            return (String)ignite.configuration().getUserAttributes().get(CHECKPOINT_KEY);
        }
    }
}
