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

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;

import static org.gridgain.examples.services.CacheServiceExampleUtils.*;

/**
 * Cache queue consumer service.
 */
public class CacheQueueConsumerService implements GridService {
    /** Injected grid. */
    @GridInstanceResource
    protected Grid grid;

    /** {@inheritDoc} */
    @Override public void cancel(GridServiceContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void execute(GridServiceContext ctx) throws Exception {
        GridCacheDataStructures ds = grid.cache(CACHE_NAME).dataStructures();

        GridCacheQueue<String> queue = ds.queue(QUEUE_NAME, QUEUE_SIZE, false, true);

        while (!ctx.isCancelled()) {
            String item = queue.take();

            System.out.println(">>> Consumer processed item: " + item);

            Thread.sleep(1000);
        }
    }
}
