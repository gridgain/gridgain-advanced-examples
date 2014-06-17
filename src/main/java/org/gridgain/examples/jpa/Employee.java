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

package org.gridgain.examples.jpa;

import javax.persistence.*;
import java.util.*;

/**
 * An entity class representing an employee working for an {@code Organization}.
 */
@Entity
class Employee {
    /** ID. */
    @Id
    @GeneratedValue(strategy=GenerationType.AUTO)
    private long id;

    /** Author. */
    @ManyToOne
    private Organization employer;

    /** First name. */
    private String firstName;

    /** Last name. */
    private String lastName;

    /** Hired timestamp. */
    private Date hired;

    /**
     * Default constructor (required by Hibernate).
     */
    Employee() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param employer Employer.
     * @param firstName First name.
     * @param lastName Last name.
     */
    Employee(Organization employer, String firstName, String lastName) {
        this.employer = employer;
        this.firstName = firstName;
        this.lastName = lastName;

        hired = new Date();
    }

    /**
     * @return ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id New ID.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return Employer.
     */
    public Organization getEmployer() {
        return employer;
    }

    /**
     * @param employer New employer.
     */
    public void setEmployer(Organization employer) {
        this.employer = employer;
    }

    /**
     * @return First name.
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * @param firstName New first name.
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * @return Last name.
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * @param lastName Last name.
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * @return Hired timestamp.
     */
    public Date getHired() {
        return hired;
    }

    /**
     * @param hired Hired timestamp.
     */
    public void setHired(Date hired) {
        this.hired = hired;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Employee [id=" + id +
            ", firstName=" + firstName +
            ", lastName=" + lastName +
            ']';
    }
}
