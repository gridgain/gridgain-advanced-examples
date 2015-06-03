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

package org.gridgain.examples.datagrid.store;

import com.google.code.morphia.*;
import com.mongodb.*;
import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.*;
import de.flapdoodle.embed.process.runtime.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.resources.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.io.*;
import java.util.*;

/**
 * Sample MongoDB embedded cache store.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheMongoStore extends CacheStoreAdapter<Long, Employee> implements LifecycleAware {
    /** MongoDB port. */
    private static final int MONGOD_PORT = 27001;

    /** MongoDB executable for embedded MongoDB store. */
    private MongodExecutable mongoExe;

    /** Mongo data store. */
    private Datastore morphia;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
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
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (mongoExe != null) {
            mongoExe.stop();

            log("Embedded mongodb stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override public Employee load(Long key) throws CacheLoaderException {
        Employee e = morphia.find(Employee.class).field("id").equal(key).get();

        log("Loaded employee: " + e);

        return e;
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends Long, ? extends Employee> e) throws CacheWriterException {
        morphia.save(e.getValue());

        log("Stored employee: " + e.getValue());
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) throws CacheWriterException {
        Employee e = morphia.find(Employee.class).field("id").equal(key).get();

        if (e != null) {
            morphia.delete(e);

            log("Removed employee: " + key);
        }
    }

    /**
     * @param msg Message.
     */
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
}
