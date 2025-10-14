package org.example.v2x.vehicle.tasks;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.example.v2x.common.config.AppConfig;
import org.example.v2x.common.net.DedupCache;
import org.example.v2x.common.util.Jsons;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RequesterTask {

    private static final Pattern TOPIC_REGION = Pattern.compile("^v2x/region/([^/]+)/data$");

    private final DedupCache dpd = new DedupCache(4096, Duration.ofSeconds(10));
    private final Path sendDir;
    private final String sendTemplate; // 例: "*{region}*.txt"

    public RequesterTask(AppConfig cfg) {
        this.sendDir = Paths.get(cfg.transferSendDir).toAbsolutePath().normalize();
        this.sendTemplate = (cfg.transferSendTemplate == null || cfg.transferSendTemplate.isBlank())
                ? "*{region}*"
                : cfg.transferSendTemplate;
        System.out.println("[Requester] SEND_DIR=" + this.sendDir + " TEMPLATE=" + this.sendTemplate);
    }

    public IMqttMessageListener asListener() {
        return this::onMessage;
    }

    private void onMessage(String topic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);

        String key = topic + "|" + Integer.toHexString(payload.hashCode());
        if (dpd.seen(key, System.currentTimeMillis())) return;

        try {
            if (payload.contains("\"points\"")) {
                System.out.println("[Requester] point-chunk seen topic=" + topic);
                return;
            }

            if (payload.contains("fetch-request")) {
                String regionId = extractRegionId(payload, topic);
                System.out.println("[Requester] fetch-request for region=" + regionId);

                @SuppressWarnings("unchecked")
                Map<String, Object> m = Jsons.GSON.fromJson(payload, Map.class);
                Object rx = m.get("rx_udp");
                if (!(rx instanceof Map)) {
                    System.out.println("[Requester] fetch-request has no rx_udp -> skip");
                    return;
                }
                String ip = String.valueOf(((Map<?, ?>) rx).get("ip"));
                int port = ((Number) ((Map<?, ?>) rx).get("port")).intValue();

                if (!Files.isDirectory(sendDir)) {
                    System.out.println("[Requester] SEND_DIR not a directory: " + sendDir);
                    return;
                }
                Path file = pickLatestForRegion(sendDir, sendTemplate, regionId);
                if (file == null) {
                    System.out.println("[Requester] no file matched for region=" + regionId + " in " + sendDir);
                    return;
                }

                byte[] data = Files.readAllBytes(file);
                udpSend(data, ip, port);
                System.out.println("[Requester] sent file=" + file + " (" + data.length + " bytes) to " + ip + ":" + port);
                return;
            }

            System.out.println("[Requester] other message topic=" + topic + " payload=" + payload);
        } catch (Exception e) {
            System.err.println("[Requester] error: " + e);
        }
    }

    private static String extractRegionId(String payload, String topic) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = Jsons.GSON.fromJson(payload, Map.class);
            Object r = (m != null) ? m.get("region_id") : null;
            if (r != null) return String.valueOf(r);
        } catch (Exception ignore) {}
        Matcher mm = TOPIC_REGION.matcher(topic);
        if (mm.matches()) return mm.group(1);
        return "unknown";
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
}
