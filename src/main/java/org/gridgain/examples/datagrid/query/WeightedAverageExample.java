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

package org.gridgain.examples.datagrid.query;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;

import java.io.*;
import java.util.*;

/**
 * This examples shows how to get weighted average with GridGain.
 * <p>
 * Example will calculate the average speed of the car going N segments of its route.
 * On each segment the speed is constant and equals to Vi, Ti is the time taken to finish the segment.
 * Weight of the segment's speed is the time of the segment.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-ignite.xml'}.
 */
public class WeightedAverageExample {
    /** Cache name. */
    private static final String CACHE_NAME = WeightedAverageExample.class.getSimpleName();

    /**
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            CacheConfiguration<Long, Segment> cc = new CacheConfiguration<>(CACHE_NAME);

            cc.setIndexedTypes(Long.class, Segment.class);

            try (IgniteCache<Long, Segment> c = ignite.createCache(cc)) {
                initialize();

                // Calculate average weighted speed.
                // Take only those segments where car was faster than 80 km/h.
                SqlFieldsQuery qry = new SqlFieldsQuery(
                    "select sum(speed * duration) / sum(duration) from Segment where speed > 80");

                Collection<List<?>> res = c.query(qry).getAll();

                assert res.size() == 1;

                double avgWeightedSpeed = (Double)res.iterator().next().get(0);

                System.out.println("Average speed is: " + avgWeightedSpeed + " km/h");
            }
        }
    }

    /**
     * Populate cache with test data.
     */
    private static void initialize() {
        Random r = new Random();

        IgniteCache<Long, Segment> c = Ignition.ignite().cache(CACHE_NAME);

        // Car went through 50 segments.
        for (long s = 0; s < 50; s++) {
            c.put(s, new Segment(
                s,
                60 * (1 + r.nextDouble()),    // Speed cannot be less than 60 km/h
                0.5 + r.nextDouble())         // Duration is 30 min and above.
            );
        }
    }

    /**
     * Class representing a segment of a car route. Segment
     * is characterized by speed and duration.
     */
    private static class Segment implements Serializable {
        /** */
        private long segmentId;

        /** */
        @QuerySqlField(index = false)
        private double speed;

        /** */
        @QuerySqlField(index = false)
        private double duration;

        /**
         * @param segmentId Segment ID.
         * @param speed Speed.
         * @param duration Duration.
         */
        private Segment(long segmentId, double speed, double duration) {
            this.segmentId = segmentId;
            this.speed = speed;
            this.duration = duration;
        }

        /**
         * @return Segment ID.
         */
        public long getSegmentId() {
            return segmentId;
        }

        /**
         * @return Speed.
         */
        public double getSpeed() {
            return speed;
        }

        /**
         * @return Duration.
         */
        public double getDuration() {
            return duration;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Segment [id=" + segmentId + ", speed=" + speed + ", duration=" + duration + ']';
        }
    }
}
