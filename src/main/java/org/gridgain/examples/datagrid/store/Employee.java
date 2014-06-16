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

import com.google.code.morphia.annotations.*;
import org.bson.types.*;

import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * Person class.
 */
@Entity("employees")
public class Employee implements Serializable {
    /** ID generator. */
    private static final AtomicLong IDGEN = new AtomicLong(System.currentTimeMillis());

    /** Required by Morphia. */
    @Id
    private ObjectId objectId;

    /** Employee ID. */
    @Indexed
    private long id;

    /** First name. */
    private String name;

    /** Salary. */
    private double salary;

    /**
     * Required by Morphia.
     */
    private Employee() {
        // No-op.
    }

    /**
     * Constructs employee record.
     *
     * @param id ID.
     * @param name First name.
     * @param salary Salary.
     */
    public Employee(long id, String name, double salary) {
        this.id = id;
        this.name = name;
        this.salary = salary;
    }

    /**
     * Constructs employee record.
     *
     * @param objectId Object ID.
     * @param id ID.
     * @param name First name.
     * @param salary Salary.
     */
    public Employee(ObjectId objectId, long id, String name, double salary) {
        this(id, name, salary);

        this.objectId = objectId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person [firstName=" + name +
            ", id=" + id +
            ", salary=" + salary + ']';
    }
}

