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

package org.gridgain.examples.compute.loadbalancing;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveJobCountLoadProbe;
import org.apache.ignite.spi.loadbalancing.adaptive.AdaptiveLoadBalancingSpi;
import org.gridgain.examples.ExampleNodeStartup;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

/**
 * Example demonstrates usage of {@link AdaptiveJobCountLoadProbe} which will delegate more jobs execution to the node
 * that is not under high load.
 *
 * The longer example is run the more difference you will see in the total number of jobs that are
 * executed on the nodes.
 *
 * To demonstrate the example start a remote node before using {@link ExampleNodeStartup}.
 */
public class ComputeJobCountBalancingExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        DefaultListableBeanFactory factory = new DefaultListableBeanFactory();

        new XmlBeanDefinitionReader(factory).loadBeanDefinitions(new FileSystemResource(
            new File("config/example-ignite.xml")));

        IgniteConfiguration cfg = factory.getBean(IgniteConfiguration.class);

        // Setting up job count adaptive load balancing SPI
        AdaptiveLoadBalancingSpi loadSpi = new AdaptiveLoadBalancingSpi();
        AdaptiveJobCountLoadProbe probe = new AdaptiveJobCountLoadProbe();

        // Use current job count load value.
        probe.setUseAverage(false);

        loadSpi.setLoadProbe(probe);
        cfg.setLoadBalancingSpi(loadSpi);


        try (Ignite ignite = Ignition.start(cfg)) {
            if (ignite.cluster().nodes().size() < 2)
                throw new RuntimeException("Not enough nodes in the topology to demonstrate the example");

            IgniteCompute compute = ignite.compute().withAsync();

            for (;;) {
                try {
                    compute.execute(new BalancingTask(), ignite.cluster().localNode().id());

                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (Exception e) {
                    if (Ignition.state(ignite.name()) != IgniteState.STOPPED)
                        e.printStackTrace();

                    break;
                }
            }
        }
    }

    /**
     * {@link ComputeTaskSplitAdapter#map(List, Object)} method's implementation will split jobs execution basing
     * on the current jobs waiting that is being waiting or being executed.
     *
     * The longer this example is being run the more jobs will be executed by the remote node because we will
     * slow down jobs execution on the node that started this example.
     */
    private static class BalancingTask extends ComputeTaskSplitAdapter <UUID, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int nodesCnt,
            UUID slowNodeId) throws IgniteException {

            ArrayList<BalancingJob> list = new ArrayList<>();

            for (int i = 0; i < nodesCnt; i++)
                list.add(new BalancingJob(slowNodeId));

            return list;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> list) throws IgniteException {
            return null;
        }
    }

    /**
     * Job which execution is slowed down on the node that started this example ({@code BalancingJob#slowNodeId}).
     * This lets {@link AdaptiveLoadBalancingSpi} to delegate more jobs to other node.
     */
    private static class BalancingJob implements ComputeJob {
        /** ID of the node that must slow down execution of jobs */
        private UUID slowNodeId;

        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Constructor.
         *
         * @param slowNodeId ID of the node that must execute jobs for long time.
         */
        public BalancingJob(UUID slowNodeId) {
            this.slowNodeId = slowNodeId;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // Ignore for this example.
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            ConcurrentMap<String, AtomicInteger> localMap = ignite.cluster().nodeLocalMap();

            AtomicInteger pending = localMap.get("pending");

            if (pending == null) {
                AtomicInteger old = localMap.putIfAbsent("pending", pending = new AtomicInteger());

                if (old != null)
                    pending = old;
            }

            pending.incrementAndGet();

            if (ignite.cluster().localNode().id().equals(slowNodeId)) {
                // Holding job execution for 15 seconds. AdaptiveLoadBalancing should delegate more jobs to the other
                // node for execution.
                try {
                    Thread.sleep(15 * 1000);
                }
                catch (InterruptedException e) {
                    // Ignore.
                }
            }

            AtomicInteger finished = localMap.get("finished");

            if (finished == null) {
                AtomicInteger old = localMap.putIfAbsent("finished", finished = new AtomicInteger());

                if (old != null)
                    finished = old;
            }

            System.out.println("Jobs stat [executed = " + finished.incrementAndGet() + ", pending = "
                + pending.decrementAndGet() + ']');

            return null;
        }
    }
}
