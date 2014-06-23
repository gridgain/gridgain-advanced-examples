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

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;

import java.io.*;
import java.util.*;
import java.util.Map.*;

/**
 * This examples shows how to get weighted average with GridGain.
 * <p>
 * Example will calculate the average speed of the car going N segments of its route.
 * On each segment the speed is constant and equals to Vi, Ti is the time taken to finish the segment.
 * Weight of the segment's speed is the time of the segment.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} ADVANCED-EXAMPLES-DIR/config/example-cache.xml'}.
 */
public class WeightedAverageExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /**
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("config/example-cache.xml")) {
            initialize();

            GridCacheProjection<Long, Segment> c = g.cache(CACHE_NAME);

            // Clear caches before running example.
            c.globalClearAll();

            // Calculate average weighted speed.
            GridCacheQuery<Entry<Long, Segment>> qry = c.queries().createSqlQuery(
                Segment.class,
                "speed > 80"); // Take only those segments where car was faster than 80 km/h.

            Collection<GridBiTuple<Double, Double>> res = qry.execute(
                new GridReducer<Entry<Long, Segment>, GridBiTuple<Double, Double>>() {
                    private double sumWeightedV;

                    private double sumT;

                    @Override public boolean collect(Entry<Long, Segment> e) {
                        Segment segment = e.getValue();

                        System.out.println("Reducing entry: " + segment);

                        sumWeightedV += segment.getSpeed() * segment.getDuration();

                        sumT += segment.getDuration();

                        // Continue collecting.
                        return true;
                    }

                    @Override public GridBiTuple<Double, Double> reduce() {
                        return new GridBiTuple<>(sumWeightedV, sumT);
                    }
                }).get();

            double v = 0.0d;
            int t = 0;

            for (GridBiTuple<Double, Double> t0 : res) {
                v += t0.get1();
                t += t0.get2();
            }

            double avgWeightedSpeed = v / t;

            System.out.println("Average speed is: " + avgWeightedSpeed + " km/h");
        }
    }

    /**
     * Populate cache with test data.
     *
     * @throws GridException In case of error.
     */
    private static void initialize() throws GridException {
        Random r = new Random();

        GridCache<Long, Segment> c = GridGain.grid().cache(CACHE_NAME);

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
        @GridCacheQuerySqlField(index = false)
        private double speed;

        /** */
        @GridCacheQuerySqlField(index = false)
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
