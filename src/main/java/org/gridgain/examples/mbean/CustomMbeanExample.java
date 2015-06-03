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

package org.gridgain.examples.mbean;

import org.apache.ignite.*;

import javax.management.*;
import javax.swing.*;

/**
 * This example demonstrates how to register custom MBean.
 * <p>
 * This example is intended for custom MBean demonstration and is not supposed to run with remote nodes.
 */
public class CustomMbeanExample {
    /**
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            ObjectName objName = new ObjectName("org.gridgain:name=Custom MBean");

            try {
                StandardMBean bean = new StandardMBean(new RandomGeneratorMBeanImpl(), RandomGeneratorMBean.class);

                ignite.configuration().getMBeanServer().registerMBean(bean, objName);

                JOptionPane.showMessageDialog(null, "Press OK to unregister the MBean and stop the node.");
            }
            finally {
                try {
                    ignite.configuration().getMBeanServer().unregisterMBean(objName);
                }
                catch (Exception ignored) {
                    // No-op.
                }
            }
        }
    }
}
