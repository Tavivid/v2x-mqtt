package org.example.v2x.common.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class AppConfig {
    public final String mqttHost;
    public final int mqttPort;
    public final String mqttClientPrefix;
    public final int udpSendPort;
    public final int udpRecvPort;
    public final int geohashPrecision;
    public final int regionTtlSeconds;
    public final String vehicleId;
    public final double publishRateHz;
    public final int maxPointsPerChunk;
    // dataset
    public final String datasetPath;
    public final boolean datasetLoop;
    public final String datasetGlob;

    @SuppressWarnings("unchecked")
    public static AppConfig load() {
        try (InputStream in = AppConfig.class.getResourceAsStream("/v2x-config.yml")) {
            if (in == null) {
                throw new IllegalStateException("v2x-config.yml not found on classpath (src/main/resources に置いてください)");
            }
            Map<String, Object> root = new Yaml().load(in);
            Map<String, Object> mqtt = (Map<String, Object>) root.get("mqtt");
            Map<String, Object> udp = (Map<String, Object>) root.get("udp");
            Map<String, Object> region = (Map<String, Object>) root.get("region");
            Map<String, Object> vehicle = (Map<String, Object>) root.get("vehicle");
            Map<String, Object> dataset = vehicle != null && vehicle.containsKey("dataset")
                    ? (Map<String, Object>) vehicle.get("dataset")
                    : java.util.Map.of();

            String host = (String) (mqtt != null ? mqtt.getOrDefault("host", "localhost") : "localhost");
            int port = ((Number) (mqtt != null ? mqtt.getOrDefault("port", 1883) : 1883)).intValue();
            String prefix = (String) (mqtt != null ? mqtt.getOrDefault("clientPrefix", "v2x-") : "v2x-");
            int send = ((Number) (udp != null ? udp.getOrDefault("send_port", 51234) : 51234)).intValue();
            int recv = ((Number) (udp != null ? udp.getOrDefault("recv_port", 51235) : 51235)).intValue();
            int precision = ((Number) (region != null ? region.getOrDefault("geohash_precision", 8) : 8)).intValue();
            int ttlSec = ((Number) (region != null ? region.getOrDefault("ttl_seconds", 5) : 5)).intValue();
            String vid = (String) (vehicle != null ? vehicle.getOrDefault("id", "vehicle-unknown") : "vehicle-unknown");
            double hz = ((Number) (vehicle != null ? vehicle.getOrDefault("publish_rate_hz", 2) : 2)).doubleValue();
            int maxPts = ((Number) (vehicle != null ? vehicle.getOrDefault("max_points_per_chunk", 400) : 400)).intValue();

            String dsPath = (String) dataset.getOrDefault("path", "./dataset");
            boolean dsLoop = dataset.getOrDefault("loop", Boolean.TRUE) instanceof Boolean b && b;
            String dsGlob = (String) dataset.getOrDefault("glob", "**/*.csv");

            return new AppConfig(host, port, prefix, send, recv, precision, ttlSec, vid, hz, maxPts, dsPath, dsLoop, dsGlob);
        } catch (IOException e) {
            // try-with-resources の close() などで発生しうる IOException を包む
            throw new UncheckedIOException("Failed to read v2x-config.yml from classpath", e);
        }
    }

    public AppConfig(String mqttHost, int mqttPort, String mqttClientPrefix,
                     int udpSendPort, int udpRecvPort, int geohashPrecision,
                     int regionTtlSeconds, String vehicleId, double publishRateHz, int maxPointsPerChunk,
                     String datasetPath, boolean datasetLoop, String datasetGlob) {
        this.mqttHost = mqttHost;
        this.mqttPort = mqttPort;
        this.mqttClientPrefix = mqttClientPrefix;
        this.udpSendPort = udpSendPort;
        this.udpRecvPort = udpRecvPort;
        this.geohashPrecision = geohashPrecision;
        this.regionTtlSeconds = regionTtlSeconds;
        this.vehicleId = vehicleId;
        this.publishRateHz = publishRateHz;
        this.maxPointsPerChunk = maxPointsPerChunk;
        this.datasetPath = datasetPath;
        this.datasetLoop = datasetLoop;
        this.datasetGlob = datasetGlob;
    }
}
