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

package org.gridgain.examples.compute.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.gridgain.examples.ExampleNodeStartup;
import org.jetbrains.annotations.Nullable;

/**
 * This example demonstrates the usage of {@link ComputeTaskSession} in the following way:
 * <p>
 * - attributes sharing across all the nodes;
 * - synchronization aids that let to stop a job execution until an attribute is set to a particular value.
 * <p>
 * To demonstrate the example start a remote node before using {@link ExampleNodeStartup}.
 */
public class ComputeJobsDistributedSynchronizationExample {
    /** */
    private static final String LEADER_ID_ATTR = "LeaderAttr";

    /** */
    private static final String LEADER_FINISHED_ATTR = "LeaderFinished";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            if (ignite.cluster().nodes().size() < 2)
                throw new RuntimeException("Not enough nodes in the topology to demonstrate the example");

            IgniteCompute compute = ignite.compute();

            String res = compute.execute(new SimpleTask(), null);

            System.out.println("Task execution result: " + res);
        }
    }

    /**
     * Assigns {@code SimpleJob} to every cluster node and elects the first one from the list as a leader by storing
     * a special flag in {@link ComputeTaskSession}.
     * <p>
     * All the nodes will be waiting while the leader starts executing its job.
     */
    @ComputeTaskSessionFullSupport
    private static class SimpleTask extends ComputeTaskAdapter<SimpleJob, String> {
        /** */
        @TaskSessionResource
        private ComputeTaskSession session;

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> list,
            SimpleJob job) throws IgniteException {

            HashMap<SimpleJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : list)
                jobs.put(new SimpleJob(), node);

            System.out.println("Mapped jobs to node [totalCnt=" + jobs.size() + ']');

            // Placing leader ID into the session.
            session.setAttribute(LEADER_ID_ATTR, list.get(0).id());

            return jobs;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String reduce(List<ComputeJobResult> list) throws IgniteException {
            StringBuilder builder = new StringBuilder("Result: ");

            for (ComputeJobResult res : list)
                builder.append(res.getData()).append(" ");

            return builder.toString();
        }
    }

    /**
     * The logic of the job is the following:
     * - if a node is the leader then it will unblock the rest of the nodes by setting a special attribute into the
     * session;
     * - if a node is an ordinary one it will be waiting until the leader sets the attribute mentioned above.
     */
    private static class SimpleJob implements ComputeJob {
        /** */
        @TaskSessionResource
        private ComputeTaskSession session;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            UUID leaderId = session.getAttribute(LEADER_ID_ATTR);

            if (leaderId.equals(ignite.cluster().localNode().id())) {
                System.out.println("Leader processed the job, unblocking waiting node ...");

                session.setAttribute(LEADER_FINISHED_ATTR, true);
            }
            else {
                System.out.println("Waiting for leader ...");

                try {
                    session.waitForAttribute(LEADER_FINISHED_ATTR, true, 0);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();

                    throw new IgniteException("Job execution failed");
                }

                System.out.println("Leader arrived.");
            }

            return ignite.cluster().localNode().order();
        }
    }
}
