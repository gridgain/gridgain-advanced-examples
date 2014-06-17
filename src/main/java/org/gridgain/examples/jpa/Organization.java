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

import org.hibernate.annotations.*;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.*;
import java.util.*;

/**
 * An organization entity class.
 */
@Entity
class Organization {
    /** ID. */
    @Id
    @GeneratedValue(strategy=GenerationType.AUTO)
    private long id;

    /** Login. */
    @NaturalId
    private String taxId;

    /** Name. */
    private String name;

    /** Posts. */
    @OneToMany(mappedBy = "employer", cascade = CascadeType.ALL)
    private Set<Employee> employees = new HashSet<>();

    /**
     * Default constructor (required by Hibernate).
     */
    Organization() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param taxId Tax ID.
     * @param name Name.
     */
    Organization(String taxId, String name) {
        this.taxId = taxId;
        this.name = name;
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
     * @return Login.
     */
    public String getTaxId() {
        return taxId;
    }

    /**
     * @param taxId Tax ID.
     */
    public void setTaxId(String taxId) {
        this.taxId = taxId;
    }

    /**
     * @return First name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name New name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Employees.
     */
    public Set<Employee> getEmployees() {
        return employees;
    }

    /**
     * @param employees New employees.
     */
    public void setEmployees(Set<Employee> employees) {
        this.employees = employees;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Organization [id=" + id +
            ", taxId=" + taxId +
            ", name=" + name +
            ']';
    }
}
