// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store;

import com.google.code.morphia.*;
import com.mongodb.*;
import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.*;
import de.flapdoodle.embed.process.runtime.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Sample MongoDB embedded cache store.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheMongoStore extends GridCacheStoreAdapter<Long, Employee> implements GridLifecycleAware {
    /** MongoDB port. */
    private static final int MONGOD_PORT = 27001;

    /** MongoDB executable for embedded MongoDB store. */
    private MongodExecutable mongoExe;

    /** Mongo data store. */
    private Datastore morphia;

    /** Logger. */
    @GridLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        MongodStarter starter = MongodStarter.getDefaultInstance();

        try {
            IMongodConfig mongoCfg = new MongodConfigBuilder().
                version(Version.Main.PRODUCTION).
                net(new Net(MONGOD_PORT, Network.localhostIsIPv6())).
                build();

            mongoExe = starter.prepare(mongoCfg);

            mongoExe.start();

            log("Embedded MongoDB started.");

            MongoClient mongo = new MongoClient("localhost", MONGOD_PORT);

            Set<Class> clss = new HashSet<>();

            Collections.addAll(clss, Employee.class);

            morphia = new Morphia(clss).createDatastore(mongo, "test");
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws GridException {
        if (mongoExe != null) {
            mongoExe.stop();

            log("Embedded mongodb stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override public Employee load(@Nullable GridCacheTx tx, Long k) throws GridException {
        Employee e = morphia.find(Employee.class).field("id").equal(k).get();

        log("Loaded employee: " + e);

        return e;
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, Long k, Employee e) throws GridException {
        morphia.save(e);

        log("Stored employee: " + e);
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, Long k) throws GridException {
        Employee e = morphia.find(Employee.class).field("id").equal(k).get();

        if (e != null) {
            morphia.delete(e);

            log("Removed employee: " + k);
        }
    }

    private void log(String msg) {
        if (log != null) {
            log.info(">>>");
            log.info(">>> " + msg);
            log.info(">>>");
        }
        else {
            System.out.println(">>>");
            System.out.println(">>> " + msg);
            System.out.println(">>>");
        }
    }

    /**
     * Test.
     *
     * @param args Command line arguments.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        CacheMongoStore store = new CacheMongoStore();

        store.start();

        try {
            store.put(null, 1L, new Employee(1L, "Jon", 1000));
            store.put(null, 2L, new Employee(2L, "Jon", 2000));
            store.put(null, 3L, new Employee(3L, "Jon", 3000));

            store.load(null, 1L);

            store.remove(null, 1L);

            store.load(null, 1L);
        }
        finally {
            store.stop();
        }
    }
}
