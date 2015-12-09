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

package org.gridgain.examples.datagrid.nodesfilter;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.gridgain.examples.ExampleNodeStartup;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

/**
 * Example show how to control cache deployment only on specific nodes using {@link CacheConfiguration#nodeFilter}.
 *
 * To see the example in action follow the following steps:
 * - Start a couple of nodes using {@link ExampleNodeStartup}
 * - Start a couple of nodes using this example.
 * - The cache that is created by this example WILL NOT be deployed on the nodes started with
 * {@link ExampleNodeStartup}.
 */
public class CacheNodeFilterExample {
    /** */
    private final static String RED_ONLY_CACHE = "red_cache";

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

        // Mark this node as a one where {@code RED_ONLY_CACHE} can be deployed.
        Map<String, Object> attrs = new HashMap<>();

        attrs.put(CacheNodeFilter.RED_NODE, true);

        cfg.setUserAttributes(attrs);

        // Starting the node.
        Ignite ignite = Ignition.start(cfg);

        if (ignite.cluster().nodes().size() < 2)
            throw new RuntimeException("Not enough nodes in the topology to demonstrate the example");

        // Configuring and starting the red cache.
        CacheConfiguration redCacheCfg = new CacheConfiguration();

        redCacheCfg.setName(RED_ONLY_CACHE);
        redCacheCfg.setNodeFilter(new CacheNodeFilter());

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(redCacheCfg);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        // Sending broadcast message to the nodes that have red cache deployed.
        IgniteCompute compute = ignite.compute(ignite.cluster().forCacheNodes(RED_ONLY_CACHE));

        compute.broadcast(new IgniteRunnable() {
            @IgniteInstanceResource
            Ignite ignite;

            @Override public void run() {
                System.out.println("I have the red cache deployed [nodeId=" + ignite.cluster().localNode().id() + ']');
            }
        });

        // Checking that the cache is still accessible from all the nodes.
        ignite.compute().broadcast(new IgniteRunnable() {
            @IgniteInstanceResource
            Ignite ignite;

            @Override public void run() {
                IgniteCache<Integer, Integer> cache = ignite.cache(RED_ONLY_CACHE);

                // Size must be 0 on the nodes where the cache is not deployed.
                System.out.println("Red cache local size: " + cache.localSize());
            }
        });
    }
}
