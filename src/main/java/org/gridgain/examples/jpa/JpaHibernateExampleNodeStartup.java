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

package org.gridgain.examples.jpa;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 * Starts up an empty node with example cache configuration.
 */
public class JpaHibernateExampleNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        Ignition.start(configuration());
    }

    /**
     * Create Grid configuration with GGFS and enabled IPC.
     *
     * @return Grid configuration.
     */
    public static IgniteConfiguration configuration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("hibernate-grid");
        cfg.setLocalHost("127.0.0.1");

        cfg.setCacheConfiguration(
            cacheConfiguration("org.hibernate.cache.spi.UpdateTimestampsCache", ATOMIC),
            cacheConfiguration("org.hibernate.cache.internal.StandardQueryCache", ATOMIC),
            cacheConfiguration(Organization.class.getName(), TRANSACTIONAL),
            cacheConfiguration(Organization.class.getName() + ".employees", TRANSACTIONAL),
            cacheConfiguration(Employee.class.getName(), TRANSACTIONAL)
        );

        return cfg;
    }

    /**
     * Create cache configuration.
     *
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return ccfg;
    }
}
