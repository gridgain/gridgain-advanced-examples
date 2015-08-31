// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.model;

import org.apache.ignite.cache.affinity.*;

import java.io.*;

/**
 * Person key to colocate all persons working for the same organization on the same node.
 *
 * @author @java.author
 * @version @java.version
 */
public class PersonKey implements Serializable {
    /** Person ID. */
    private final long id;

    /** Organization ID. */
    @AffinityKeyMapped
    private final long orgId;

    /**
     * @param id Person ID.
     * @param orgId Organization ID.
     */
    public PersonKey(long id, long orgId) {
        this.id = id;
        this.orgId = orgId;
    }

    /**
     * @return Person ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @return Organization ID.
     */
    public long getOrganizationId() {
        return orgId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof PersonKey))
            return false;

        PersonKey personKey = (PersonKey)o;

        return id == personKey.id && orgId == personKey.orgId;
    }

    /** {@inheritDoc} */
    @Override  public int hashCode() {
        int res = (int)(id ^ (id >>> 32));

        res = 31 * res + (int)(orgId ^ (orgId >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return Long.toString(id);
    }
}
