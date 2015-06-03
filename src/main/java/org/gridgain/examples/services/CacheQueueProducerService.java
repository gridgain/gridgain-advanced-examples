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

package org.gridgain.examples.services;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.services.*;

import static org.gridgain.examples.services.CacheServiceExampleUtils.*;

/**
 * Cache queue producer service.
 */
public class CacheQueueProducerService implements Service {
    /** Injected grid. */
    @IgniteInstanceResource
    protected Ignite ignite;

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        IgniteQueue<String> queue = ignite.queue(QUEUE_NAME, QUEUE_SIZE, new CollectionConfiguration());

        IgniteAtomicLong counter = ignite.atomicLong("example-atomic", 0, true);

        while (!ctx.isCancelled()) {
            String item = "Item-" + counter.get();

            queue.put(item);

            System.out.println(">>> Producer put item: " + item);

            counter.incrementAndGet();
        }
    }
}
