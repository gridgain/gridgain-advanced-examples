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

package org.gridgain.examples.mbean;

import org.gridgain.grid.*;

import javax.management.*;
import javax.swing.*;

/**
 *
 */
public class CustomMbeanExample {
    /**
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            ObjectName objName = new ObjectName("org.gridgain:name=Custom MBean");

            try {
                StandardMBean bean = new StandardMBean(new RandomGeneratorMBeanImpl(), RandomGeneratorMBean.class);

                g.configuration().getMBeanServer().registerMBean(bean, objName);

                JOptionPane.showMessageDialog(null, "Press OK to unregister the MBean and stop the node.");
            }
            finally {
                try {
                    g.configuration().getMBeanServer().unregisterMBean(objName);
                }
                catch (Exception ignored) {
                    // No-op.
                }
            }
        }
    }
}
