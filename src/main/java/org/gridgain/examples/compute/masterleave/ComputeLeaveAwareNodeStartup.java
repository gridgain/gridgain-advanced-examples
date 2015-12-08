package org.gridgain.examples.compute.masterleave;

import java.util.Arrays;
import java.util.HashMap;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Starts a server node with specific configuration.
 *
 * Refer to {@link ComputeMasterLeaveAwareExample} for details.
 */
public class ComputeLeaveAwareNodeStartup {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        HashMap<String, Object> attrs = new HashMap<>();
        attrs.put("checkpoint_key", "slave_node");

        cfg.setUserAttributes(attrs);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // Configuring checkpoints spi.
        CacheCheckpointSpi checkpointSpi = new CacheCheckpointSpi();

        checkpointSpi.setCacheName("checkpoints");

        // Overriding default checkpoints SPI
        cfg.setCheckpointSpi(checkpointSpi);

        Ignition.start(cfg);
    }
}
