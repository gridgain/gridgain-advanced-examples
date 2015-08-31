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

package org.gridgain.examples.model;

import org.apache.ignite.cache.query.annotations.*;

import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * Person class.
 */
public class Person implements Serializable {
    /** ID generator. */
    private static final AtomicLong IDGEN = new AtomicLong();

    /** Person ID (indexed). */
    @QuerySqlField(index = true)
    private long id;

    /** Organization ID (indexed). */
    @QuerySqlField(index = true)
    private long orgId;

    /** First name (not-indexed). */
    @QuerySqlField
    private String firstName;

    /** Last name (not indexed). */
    @QuerySqlField
    private String lastName;

    /** Resume text (create LUCENE-based TEXT index for this field). */
    @QueryTextField
    private String resume;

    /** Salary (create non-unique SQL index for this field). */
    @QuerySqlField
    private double salary;

    /** Affinity-aware key. */
    private transient PersonKey key;

    /**
     * Constructs person record.
     *
     * @param org Organization.
     * @param firstName First name.
     * @param lastName Last name.
     * @param salary Salary.
     * @param resume Resume text.
     */
    public Person(Organization org, String firstName, String lastName, double salary, String resume) {
        // Generate unique ID for this person.
        id = IDGEN.incrementAndGet();

        orgId = org.getId();

        this.firstName = firstName;
        this.lastName = lastName;
        this.resume = resume;
        this.salary = salary;
    }

    /**
     * @param p Person to copy.
     */
    public Person(Person p) {
        id = p.getId();
        orgId = p.getOrganizationId();
        firstName = p.getFirstName();
        lastName = p.getLastName();
        resume = p.getResume();
        salary = p.getSalary();
    }

    /**
     * @return Affinity-aware person key.
     */
    public PersonKey key() {
        if (key == null)
            key = new PersonKey(id, orgId);

        return key;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getOrganizationId() {
        return orgId;
    }

    public void setOrganizationId(long orgId) {
        this.orgId = orgId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getResume() {
        return resume;
    }

    public void setResume(String resume) {
        this.resume = resume;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
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

