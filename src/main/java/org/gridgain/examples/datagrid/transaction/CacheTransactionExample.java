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

package org.gridgain.examples.datagrid.transaction;

import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import java.io.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Demonstrates how to use cache transactions.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}
 * or {@link CacheExampleNodeStartup} can be used.
 */
public class CacheTransactionExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache transaction example started.");

            GridCache<Long, Account> cache = g.cache(CACHE_NAME);

            // Clear cache before running example.
            cache.globalClearAll();

            // Initialize.
            cache.putx(1L, new Account(1, 100));

            System.out.println();
            System.out.println(">>> Account before deposit: ");
            System.out.println(">>> " + cache.get(1L));

            // Deposit $200 to account within a transaction.
            deposit(1, 200);

            System.out.println();
            System.out.println(">>> Account after deposit: ");
            System.out.println(">>> " + cache.get(1L));

            System.out.println(">>> Cache transaction example finished.");
        }
    }

    /**
     * Deposit money to the account with given ID.
     *
     * @param toId 'To' account ID.
     * @param amount Amount to transfer.
     * @throws GridException If failed.
     */
    private static void deposit(long toId, double amount) throws GridException {
        // Clone every object we get from cache, so we can freely update it.
        GridCacheProjection<Long, Account> cache = GridGain.grid().<Long, Account>cache(CACHE_NAME).flagsOn(CLONE);

        try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // In PESSIMISTIC mode cache objects are locked
            // automatically upon access within a transaction.
            Account acct = cache.get(toId); // Lock 'from' account first.

            assert acct != null;

            acct.update(amount);

            // Store updated account in cache.
            cache.putx(toId, acct);

            tx.commit();
        }

        System.out.println();
        System.out.println(">>> Deposit amount: $" + amount);
    }

    /**
     * Account.
     */
    private static class Account implements Serializable, Cloneable {
        /** Account ID. */
        private long id;

        /** Account balance. */
        private double balance;

        /**
         * @param id Account ID.
         * @param balance Balance.
         */
        Account(long id, double balance) {
            this.id = id;
            this.balance = balance;
        }

        /**
         * Change balance by specified amount.
         *
         * @param amount Amount to add to balance (may be negative).
         */
        void update(double amount) {
            balance += amount;
        }

        /** {@inheritDoc} */
        @Override protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Account [id=" + id + ", balance=$" + balance + ']';
        }
    }
}
