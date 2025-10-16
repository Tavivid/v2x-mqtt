package org.example.v2x.vehicle.tasks;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.example.v2x.common.config.AppConfig;
import org.example.v2x.common.model.PointCloudChunk;
import org.example.v2x.common.net.DedupCache;
import org.example.v2x.common.net.Topics;
import org.example.v2x.common.util.Jsons;
import org.example.v2x.vehicle.datasource.PointCloudSource;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.*;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PublisherTask implements Runnable {

    private static final Pattern TOPIC_REGION = Pattern.compile("^v2x/region/([^/]+)/data$");

    private final MqttClient mqtt;
    private final String vehicleId;
    private final String regionId;
    private final double publishRateHz;
    private final int maxPointsPerChunk;
    private final PointCloudSource source;
    private final DedupCache dpd = new DedupCache(2048, Duration.ofSeconds(10));

    private final Path sendDir;
    private final String sendTemplate;

    public PublisherTask(MqttClient mqtt, String vehicleId, String regionId,
            double publishRateHz, int maxPointsPerChunk, PointCloudSource source, AppConfig cfg) {
        this.mqtt = mqtt;
        this.vehicleId = vehicleId;
        this.regionId = regionId;
        this.publishRateHz = publishRateHz;
        this.maxPointsPerChunk = maxPointsPerChunk;
        this.source = source;

        this.sendDir = Paths.get(cfg.transferSendDir).toAbsolutePath().normalize();
        this.sendTemplate = (cfg.transferSendTemplate == null || cfg.transferSendTemplate.isBlank())
                ? "*{region}*"
                : cfg.transferSendTemplate;
        System.out.println("[Publisher] SEND_DIR=" + sendDir + " TEMPLATE=" + sendTemplate);

    }

    public IMqttMessageListener asListener() {
        return this::onFetchMessage;
    }

    // ====== fetch-request 受信ハンドラ ======
    private void onFetchMessage(String topic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);

        // ポイントチャンクなどデータ本体はスキップ
        if (payload.contains("\"points\"")) return;

        String dedupKey = topic + "|" + Integer.toHexString(payload.hashCode());
        if (dpd.seen(dedupKey, System.currentTimeMillis())) return;

        try {
            if (!payload.contains("fetch-request")) return;

            // regionId を JSON か topic から取得
            String region = extractRegionId(payload, topic);
            System.out.println("[Publisher] fetch-request for region=" + region);

            @SuppressWarnings("unchecked")
            Map<String, Object> obj = Jsons.GSON.fromJson(payload, Map.class);
            Object rx = (obj != null) ? obj.get("rx_udp") : null;
            if (!(rx instanceof Map)) {
                System.out.println("[Publisher] fetch-request has no rx_udp -> skip");
                return;
            }
            String ip = String.valueOf(((Map<?, ?>) rx).get("ip"));
            int port = ((Number) ((Map<?, ?>) rx).get("port")).intValue();

            // ファイル選択→UDP 送信
            handleFetch(region, ip, port);
        } catch (Exception e) {
            System.err.println("[Publisher] fetch-parse error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String extractRegionId(String payload, String topic) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = Jsons.GSON.fromJson(payload, Map.class);
            Object rid = (m != null) ? m.get("regionId") : null;
            if (rid != null) return String.valueOf(rid);
        } catch (Exception ignore) {}
        Matcher mm = TOPIC_REGION.matcher(topic);
        if (mm.matches()) return mm.group(1);
        return "unknown";
    }

    // ====== ファイル選択→UDP 送信 ======
    private void handleFetch(String region, String ip, int port) {
        try {
            if (!Files.isDirectory(sendDir)) {
                System.out.println("[Publisher] SEND_DIR not a directory: " + sendDir);
                return;
            }
            Path file = pickLatestForRegion(sendDir, sendTemplate, region);
            if (file == null) {
                System.out.println("[Publisher] no file matched for region=" + region + " in " + sendDir);
                return;
            }
            byte[] data = Files.readAllBytes(file);
            udpSend(data, ip, port);

            // ★ ファイル名は Publisher 側で出力（要求に合わせた責務）
            System.out.println("[Publisher] sent file=" + file.getFileName()
                    + " (" + data.length + " bytes) to " + ip + ":" + port
                    + " for region=" + region);
        } catch (Exception e) {
            System.err.println("[Publisher] fetch-send error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Path pickLatestForRegion(Path dir, String template, String region) throws java.io.IOException {
        String pattern = (template == null || template.isBlank()) ? "*{region}*" : template;
        pattern = pattern.replace("{region}", region);
        PathMatcher matcher = dir.getFileSystem().getPathMatcher("glob:" + pattern);
        try (var stream = Files.list(dir)) {
            return stream
                    .filter(p -> Files.isRegularFile(p) && matcher.matches(p.getFileName()))
                    .max(Comparator.comparingLong(p -> {
                        try { return Files.getLastModifiedTime(p).toMillis(); }
                        catch (Exception e) { return Long.MIN_VALUE; }
                    }))
                    .orElse(null);
        }
    }

    private static void udpSend(byte[] data, String ip, int port) throws Exception {
        try (DatagramSocket sock = new DatagramSocket()) {
            DatagramPacket pkt = new DatagramPacket(data, data.length, new InetSocketAddress(ip, port));
            sock.send(pkt);
        }
    }

    @Override
    public void run() {
        long intervalMs = (publishRateHz > 0) ? (long) (1000.0 / publishRateHz) : 500L;
        
        while (true) {
            try {
                // availability アナウンス
                var avail = Jsons.GSON.toJson(java.util.Map.of("vehicleId", vehicleId));
                mqtt.publish(Topics.availability(regionId), new MqttMessage(avail.getBytes(StandardCharsets.UTF_8)));

                // データセットが与えられている場合のみ点群 publish（不要なら source を null に）
                if (source != null && publishRateHz > 0) {
                    PointCloudChunk chunk = source.nextChunk(regionId, vehicleId, maxPointsPerChunk);
                    if (chunk != null) {
                        String key = chunk.makeDedupKey();
                        if (!dpd.seen(key, System.currentTimeMillis())) {
                            String json = Jsons.GSON.toJson(chunk);
                            mqtt.publish(Topics.data(regionId), new MqttMessage(json.getBytes(StandardCharsets.UTF_8)));
                        }
                    } else {
                        // データが枯渇したときは少し待つ
                        TimeUnit.MILLISECONDS.sleep(500);
                    }
                }

                TimeUnit.MILLISECONDS.sleep(intervalMs);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
