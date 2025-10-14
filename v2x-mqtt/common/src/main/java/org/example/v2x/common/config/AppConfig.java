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

    public final String transferSendDir;        // 送信用ディレクトリ
    public final String transferSendTemplate;   // 送信ファイル名テンプレート（{region} 置換）
    public final String transferRecvDir;        // 受信用ディレクトリ

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
            Map<String, Object> transfer = vehicle != null && vehicle.containsKey("transfer")
                    ? (Map<String, Object>) vehicle.get("transfer")
                    : java.util.Map.of();

            // YAML 既定値
            String host = (String) (mqtt != null ? mqtt.getOrDefault("host", "localhost") : "localhost");
            int port = ((Number) (mqtt != null ? mqtt.getOrDefault("port", 1883) : 1883)).intValue();
            String prefix = (String) (mqtt != null ? mqtt.getOrDefault("clientPrefix", "v2x-") : "v2x-");
            int send = ((Number) (udp != null ? udp.getOrDefault("send_port", 51234) : 51234)).intValue();
            int recv = ((Number) (udp != null ? udp.getOrDefault("recv_port", 51235) : 51235)).intValue();
            int precision = ((Number) (region != null ? region.getOrDefault("geohash_precision", 8) : 8)).intValue();
            int ttlSec = ((Number) (region != null ? region.getOrDefault("ttl_seconds", 5) : 5)).intValue();
            String vidYaml = (String) (vehicle != null ? vehicle.getOrDefault("id", "vehicle-unknown") : "vehicle-unknown");
            double hz = ((Number) (vehicle != null ? vehicle.getOrDefault("publish_rate_hz", 2) : 2)).doubleValue();
            int maxPts = ((Number) (vehicle != null ? vehicle.getOrDefault("max_points_per_chunk", 400) : 400)).intValue();

            String dsPath = (String) dataset.getOrDefault("path", "./dataset");
            boolean dsLoop = dataset.getOrDefault("loop", Boolean.TRUE) instanceof Boolean b && b;
            String dsGlob = (String) dataset.getOrDefault("glob", "**/*.csv");

            // ★ transfer の既定値
            String sendDir = (String) transfer.getOrDefault("send_dir", "./v2x-send");
            String sendTpl = (String) transfer.getOrDefault("send_file_template", "*{region}*");
            String recvDir = (String) transfer.getOrDefault("recv_dir", "./v2x-recv");

            // ===== ここから上書き（環境変数 > システムプロパティ > YAML） =====
            // 最低限：VEHICLE_ID / -DvehicleId をサポート（質問の主目的）
            String vid = envOrProp("VEHICLE_ID", "vehicleId", vidYaml);

            // 便利オプション（任意）: MQTT_HOST/MQTT_PORT/MQTT_CLIENT_PREFIX も受け付ける
            host   = envOrProp("MQTT_HOST", "mqtt.host", host);
            port   = envOrPropInt("MQTT_PORT", "mqtt.port", port);
            prefix = envOrProp("MQTT_CLIENT_PREFIX", "mqtt.clientPrefix", prefix);

            // dataset の上書きもあると実験しやすい（任意）
            dsPath = envOrProp("DATASET_PATH", "dataset.path", dsPath);
            dsLoop = envOrPropBool("DATASET_LOOP", "dataset.loop", dsLoop);
            dsGlob = envOrProp("DATASET_GLOB", "dataset.glob", dsGlob);
            // ============================================================

            return new AppConfig(host, port, prefix, send, recv, precision, ttlSec, vid, hz, maxPts,
                    dsPath, dsLoop, dsGlob,
                    sendDir, sendTpl, recvDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read v2x-config.yml from classpath", e);
        }
    }

    public AppConfig(String mqttHost, int mqttPort, String mqttClientPrefix,
                     int udpSendPort, int udpRecvPort, int geohashPrecision,
                     int regionTtlSeconds, String vehicleId, double publishRateHz, int maxPointsPerChunk,
                     String datasetPath, boolean datasetLoop, String datasetGlob,
                     String transferSendDir, String transferSendTemplate, String transferRecvDir) {
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
        this.transferSendDir = transferSendDir;
        this.transferSendTemplate = transferSendTemplate;
        this.transferRecvDir = transferRecvDir;
    }

    // ========= 上書きユーティリティ =========
    private static String envOrProp(String env, String prop, String fallback) {
        String v = System.getenv(env);
        if (v != null && !v.isBlank()) return v;
        v = System.getProperty(prop);
        return (v != null && !v.isBlank()) ? v : fallback;
    }

    private static int envOrPropInt(String env, String prop, int fallback) {
        String ev = System.getenv(env);
        if (ev != null && !ev.isBlank()) {
            try { return Integer.parseInt(ev.trim()); } catch (NumberFormatException ignore) {}
        }
        String pv = System.getProperty(prop);
        if (pv != null && !pv.isBlank()) {
            try { return Integer.parseInt(pv.trim()); } catch (NumberFormatException ignore) {}
        }
        return fallback;
    }

    private static boolean envOrPropBool(String env, String prop, boolean fallback) {
        String ev = System.getenv(env);
        if (ev != null && !ev.isBlank()) {
            return parseBool(ev.trim(), fallback);
        }
        String pv = System.getProperty(prop);
        if (pv != null && !pv.isBlank()) {
            return parseBool(pv.trim(), fallback);
        }
        return fallback;
    }

    private static boolean parseBool(String s, boolean fallback) {
        switch (s.toLowerCase()) {
            case "1": case "true": case "yes": case "y": return true;
            case "0": case "false": case "no":  case "n": return false;
            default: return fallback;
        }
    }
}
