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

package org.gridgain.examples.datagrid;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Store that collects all values being stored in memory.
 */
public class CacheMapStore<K, V> implements GridCacheStore<K, V> {
    /** */
    private Map<K, V> storeMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Nullable @Override public V load(@Nullable GridCacheTx gridCacheTx, K k) throws GridException {
        return storeMap.get(k);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(GridBiInClosure<K, V> loadClos, @Nullable Object... args) throws GridException {
        for (Map.Entry<K, V> entry : storeMap.entrySet()) {
            loadClos.apply(entry.getKey(), entry.getValue());
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable GridCacheTx gridCacheTx, Collection<? extends K> keys,
        GridBiInClosure<K, V> loadClos) throws GridException {
        for (K key : keys) {
            V val = storeMap.get(key);

            if (val != null)
                loadClos.apply(key, val);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx gridCacheTx, K k, V v) throws GridException {
        storeMap.put(k, v);
    }

    /** {@inheritDoc} */
    @Override public void putAll(@Nullable GridCacheTx gridCacheTx, Map<? extends K, ? extends V> map)
        throws GridException {
        storeMap.putAll(map);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx gridCacheTx, K key) throws GridException {
        storeMap.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(@Nullable GridCacheTx gridCacheTx, Collection<? extends K> keys)
        throws GridException {
        for (K key : keys)
            storeMap.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx gridCacheTx, boolean b) throws GridException {
        // No-op.
    }

    /**
     * @return Map containing all values stored.
     */
    public Map<K, V> storeMap() {
        return storeMap;
    }
}
