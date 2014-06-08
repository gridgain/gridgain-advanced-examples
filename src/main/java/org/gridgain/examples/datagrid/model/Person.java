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

package org.gridgain.examples.datagrid.model;

import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;

import java.io.*;
import java.util.*;

/**
 * Person class.
 */
public class Person implements Serializable {
    /** Person ID (indexed). */
    @GridCacheQuerySqlField(index = true)
    private UUID id;

    /** Organization ID (indexed). */
    @GridCacheQuerySqlField(index = true)
    private UUID orgId;

    /** First name (not-indexed). */
    @GridCacheQuerySqlField
    private String firstName;

    /** Last name (not indexed). */
    @GridCacheQuerySqlField
    private String lastName;

    /** Resume text (create LUCENE-based TEXT index for this field). */
    @GridCacheQueryTextField
    private String resume;

    /** Salary (create non-unique SQL index for this field). */
    @GridCacheQuerySqlField
    private double salary;

    /** Custom cache key to guarantee that person is always collocated with its organization. */
    private transient GridCacheAffinityKey<UUID> key;

    /**
     * Constructs person record.
     *
     * @param org Organization.
     * @param firstName First name.
     * @param lastName Last name.
     * @param salary Salary.
     * @param resume Resume text.
     */
    Person(Organization org, String firstName, String lastName, double salary, String resume) {
        // Generate unique ID for this person.
        id = UUID.randomUUID();

        orgId = org.id();

        this.firstName = firstName;
        this.lastName = lastName;
        this.resume = resume;
        this.salary = salary;
    }

    /**
     * Gets cache affinity key. Since in some examples person needs to be collocated with organization, we create
     * custom affinity key to guarantee this collocation.
     *
     * @return Custom affinity key to guarantee that person is always collocated with organization.
     */
    public GridCacheAffinityKey<UUID> key() {
        if (key == null)
            key = new GridCacheAffinityKey<>(id, orgId);

        return key;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person [firstName=" + firstName +
            ", id=" + id +
            ", orgId=" + orgId +
            ", lastName=" + lastName +
            ", resume=" + resume +
            ", salary=" + salary + ']';
    }
}

