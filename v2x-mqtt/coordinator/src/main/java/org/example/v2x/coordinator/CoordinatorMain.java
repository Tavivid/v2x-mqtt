package org.example.v2x.coordinator;

import org.example.v2x.common.config.AppConfig;

public class CoordinatorMain {

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();
        RegionDirectory dir = new RegionDirectory(cfg.regionTtlSeconds * 1000L);
        new SubscriptionManager(cfg, dir); // subscribes and runs via MQTT callback thread
        new Thread(new UdpRelayServer(cfg.udpRecvPort), "UdpRelay").start();
        System.out.println("Coordinator started.");
    }
}
