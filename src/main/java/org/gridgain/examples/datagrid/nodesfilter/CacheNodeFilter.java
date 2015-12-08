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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Cache filter that is used to deploy a cache on the nodes that has an entry for {@code RED_NODE} in their attributes.
 *
 * Used by {@link CacheNodeFilterExample} example.
 */
public class CacheNodeFilter implements IgnitePredicate<ClusterNode> {
    /** */
    public static String RED_NODE = "RED_NODE";

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode node) {
        // Cache will be deployed only on the nodes that has this attribute being set.
        return node.attributes().containsKey(RED_NODE);
    }
}
