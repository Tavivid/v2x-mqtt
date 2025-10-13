package org.example.v2x.vehicle;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.example.v2x.common.config.AppConfig;
import org.example.v2x.common.geo.GeoHash;
import org.example.v2x.vehicle.datasource.DatasetPointCloudSource;
import org.example.v2x.vehicle.datasource.PointCloudSource;
import org.example.v2x.vehicle.net.MqttClientFactory;
import org.example.v2x.vehicle.tasks.PublisherTask;
import org.example.v2x.vehicle.tasks.RequesterTask;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * VehicleMain
 * - 送信用CSV（/request）と動的購読CSV（/data）を分離
 * - ★初期固定購読（RequesterTask#run）は行わない
 * - ★動的購読のリスナー処理は RequesterTask に一本化（asListener）
 * - アイドルで自動unsubscribe: SUB_IDLE_MS=ミリ秒（既定 5000）
 */
public class VehicleMain {

    // ===== 動的購読マネージャ（最小実装：/data 固定） =====
    static final class DynamicSubscriptionManager implements AutoCloseable {
        private final MqttClient client;
        private final long idleMs;
        private final IMqttMessageListener listener;
        private final ConcurrentMap<String, Long> lastTouched = new ConcurrentHashMap<>();
        private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dyn-sub-pruner"); t.setDaemon(true); return t;
        });

        DynamicSubscriptionManager(MqttClient client, long idleMs, IMqttMessageListener listener) {
            this.client = client;
            this.idleMs = idleMs;
            this.listener = listener;
        }
        void start() {
            ses.scheduleAtFixedRate(this::pruneIdle, idleMs, Math.max(1000L, idleMs / 3), TimeUnit.MILLISECONDS);
        }
        void touch(String region) {
            long now = System.currentTimeMillis();
            lastTouched.put(region, now);
            if (!subscribed.contains(region)) {
                String topic = "v2x/region/" + region + "/data";
                try {
                    client.subscribe(topic, /*qos*/0, listener);
                    subscribed.add(region);
                    System.out.println("[DYN] subscribed " + topic);
                } catch (Exception e) {
                    System.err.println("[DYN] subscribe failed topic=" + topic + " err=" + e);
                }
            }
        }
        void unsubscribe(String region) {
            String topic = "v2x/region/" + region + "/data";
            try {
                client.unsubscribe(topic);
                subscribed.remove(region);
                lastTouched.remove(region);
                System.out.println("[DYN] unsubscribed " + topic);
            } catch (Exception e) {
                System.err.println("[DYN] unsubscribe failed topic=" + topic + " err=" + e);
            }
        }
        private void pruneIdle() {
            long now = System.currentTimeMillis();
            for (Map.Entry<String, Long> e : lastTouched.entrySet()) {
                if (now - e.getValue() >= idleMs) unsubscribe(e.getKey());
            }
        }
        @Override public void close() {
            ses.shutdownNow();
            for (String r : new ArrayList<>(subscribed)) { try { unsubscribe(r); } catch (Exception ignore) {} }
        }
    }
    // ========================================

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();

        // （Publisher 用などで参照は可。固定購読には使わない）
        String region = System.getProperty("region", null);
        if (region == null || region.isBlank()) {
            region = GeoHash.encode(35.681236, 139.767125, cfg.geohashPrecision);
        }

        // MQTT 接続
        MqttClient client = MqttClientFactory.connect(cfg.mqttHost, cfg.mqttPort, cfg.mqttClientPrefix + cfg.vehicleId);

        // Publisher（データセットがあれば起動）
        PointCloudSource source = null;
        boolean startPublisher = true;
        try {
            source = new DatasetPointCloudSource(cfg.datasetPath, cfg.datasetGlob, cfg.datasetLoop);
        } catch (Exception e) {
            System.err.println("[Vehicle] Dataset not available: " + e.getMessage());
            startPublisher = false;
        }
        if (startPublisher && source != null) {
            new Thread(new PublisherTask(client, cfg.vehicleId, region, cfg.publishRateHz, cfg.maxPointsPerChunk, source),
                    "Publisher").start();
        } else {
            System.out.println("[Vehicle] No dataset -> PublisherTask is not started.");
        }

        // ★RequesterTask を「リスナー提供者」としてだけ使う（初期固定購読はしない）
        RequesterTask reqHandler = new RequesterTask(client, region); // regionはダミーでOK（内部で使わない前提）
        IMqttMessageListener dynListener = reqHandler.asListener();

        // 動的購読マネージャ（/data 固定）
        long idleMs = Long.parseLong(System.getenv().getOrDefault("SUB_IDLE_MS", "50000"));
        DynamicSubscriptionManager dynSub = new DynamicSubscriptionManager(client, idleMs, dynListener);
        dynSub.start();

        System.out.println("Vehicle started (dynamic-subscribe only; handler=RequesterTask) defaultRegion=" + region
                + ", dataset=" + cfg.datasetPath);

        // ====== 送信用CSV（REQ_FEED_CSV）: at_ms,region_id,ip,port ======
        String reqCsvPath = System.getenv("REQ_FEED_CSV");
        boolean reqLoop = "1".equals(System.getenv("REQ_FEED_LOOP"));

        if (reqCsvPath != null && !reqCsvPath.isBlank()) {
            File reqCsv = new File(reqCsvPath);
            if (!reqCsv.exists()) {
                System.err.println("[FEED-REQ] CSV not found: " + reqCsv.getAbsolutePath());
            } else {
                Thread reqFeeder = new Thread(() -> {
                    do {
                        runRequestFeeder(reqCsv, client); // 要求送信のみ（購読は付随させない）
                        if (!reqLoop) break;
                        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                    } while (true);
                }, "request-feeder");
                reqFeeder.setDaemon(true);
                reqFeeder.start();
                System.out.println("[FEED-REQ] started file=" + reqCsv.getAbsolutePath() + " loop=" + reqLoop);
            }
        } else {
            System.out.println("[FEED-REQ] REQ_FEED_CSV not set -> no /request will be published.");
        }

        // ====== 購読用CSV（SUB_FEED_CSV）: at_ms,region_id ======
        String subCsvPath = System.getenv("SUB_FEED_CSV");
        boolean subLoop = "1".equals(System.getenv("SUB_FEED_LOOP"));

        if (subCsvPath != null && !subCsvPath.isBlank()) {
            File subCsv = new File(subCsvPath);
            if (!subCsv.exists()) {
                System.err.println("[FEED-SUB] CSV not found: " + subCsv.getAbsolutePath());
            } else {
                Thread subFeeder = new Thread(() -> {
                    do {
                        runSubscribeFeeder(subCsv, dynSub); // 指定リージョンのみ動的購読
                        if (!subLoop) break;
                        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                    } while (true);
                }, "subscribe-feeder");
                subFeeder.setDaemon(true);
                subFeeder.start();
                System.out.println("[FEED-SUB] started file=" + subCsv.getAbsolutePath() + " loop=" + subLoop);
            }
        } else {
            System.out.println("[FEED-SUB] SUB_FEED_CSV not set -> no dynamic /data subscriptions will be added.");
        }

        // 設定ファイル存在確認
        try (InputStream in = VehicleMain.class.getResourceAsStream("/v2x-config.yml")) {
            if (in == null) System.err.println("WARNING: v2x-config.yml not found in classpath!");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(dynSub::close));
    }

    // ======= 送信用フィーダ（/request を出す） =======
    private static void runRequestFeeder(File csv, MqttClient client) {
        try {
            List<RowReq> rows = loadReqCsv(csv);
            if (rows.isEmpty()) { System.out.println("[FEED-REQ] no rows."); return; }
            long start = System.currentTimeMillis();
            for (RowReq r : rows) {
                long due = start + r.atMs;
                long now = System.currentTimeMillis();
                if (due > now) TimeUnit.MILLISECONDS.sleep(due - now);
                publishRequest(client, r.regionId, r.rx); // 購読は付随させない
            }
            System.out.println("[FEED-REQ] sequence done");
        } catch (Exception e) {
            System.err.println("[FEED-REQ] error: " + e);
        }
    }

    // ======= 購読用フィーダ（動的に /data を追加） =======
    private static void runSubscribeFeeder(File csv, DynamicSubscriptionManager dynSub) {
        try {
            List<RowSub> rows = loadSubCsv(csv);
            if (rows.isEmpty()) { System.out.println("[FEED-SUB] no rows."); return; }
            long start = System.currentTimeMillis();
            for (RowSub r : rows) {
                long due = start + r.atMs;
                long now = System.currentTimeMillis();
                if (due > now) TimeUnit.MILLISECONDS.sleep(due - now);
                dynSub.touch(r.regionId);
                System.out.println("[FEED-SUB] touch region=" + r.regionId);
            }
            System.out.println("[FEED-SUB] sequence done");
        } catch (Exception e) {
            System.err.println("[FEED-SUB] error: " + e);
        }
    }

    private static void publishRequest(MqttClient client, String regionId, InetSocketAddress rx) {
        try {
            long now = System.currentTimeMillis();
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("type", "need-pointcloud");
            body.put("region_id", regionId);
            body.put("rx_udp", Map.of("ip", rx.getAddress().getHostAddress(), "port", rx.getPort()));
            body.put("ts_ms", now);
            body.put("nonce", UUID.randomUUID().toString());

            byte[] payload = new ObjectMapper().writeValueAsBytes(body);
            MqttMessage msg = new MqttMessage(payload);
            msg.setQos(0);
            msg.setRetained(false);
            String topic = "v2x/region/" + regionId + "/request";
            client.publish(topic, msg);
            System.out.printf("[FEED-REQ] published topic=%s payload=%s%n",
                    topic, new String(payload, StandardCharsets.UTF_8));
        } catch (Exception e) {
            System.err.println("[FEED-REQ] publish failed: " + e);
        }
    }

    // ======= CSV ローダ（送信用: at_ms,region_id,ip,port） =======
    private static List<RowReq> loadReqCsv(File file) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            List<RowReq> out = new ArrayList<>();
            String line; long lineno = 0;
            while ((line = br.readLine()) != null) {
                lineno++; line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] tk = line.split(",", -1);
                if (tk.length < 4)
                    throw new IllegalArgumentException("REQ CSV format error at line " + lineno + " (expect: at_ms,region_id,ip,port)");
                long at = Long.parseLong(tk[0].trim());
                String regionId = tk[1].trim();
                String ip = tk[2].trim();
                int port = Integer.parseInt(tk[3].trim());
                out.add(new RowReq(at, regionId, new InetSocketAddress(ip, port)));
            }
            return out.stream().sorted(Comparator.comparingLong(r -> r.atMs)).collect(Collectors.toList());
        }
    }

    // ======= CSV ローダ（購読用: at_ms,region_id） =======
    private static List<RowSub> loadSubCsv(File file) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            List<RowSub> out = new ArrayList<>();
            String line; long lineno = 0;
            while ((line = br.readLine()) != null) {
                lineno++; line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] tk = line.split(",", -1);
                if (tk.length < 2)
                    throw new IllegalArgumentException("SUB CSV format error at line " + lineno + " (expect: at_ms,region_id)");
                long at = Long.parseLong(tk[0].trim());
                String regionId = tk[1].trim();
                out.add(new RowSub(at, regionId));
            }
            return out.stream().sorted(Comparator.comparingLong(r -> r.atMs)).collect(Collectors.toList());
        }
    }

    // ======= CSV行モデル =======
    private static final class RowReq {
        final long atMs; final String regionId; final InetSocketAddress rx;
        RowReq(long atMs, String regionId, InetSocketAddress rx) { this.atMs = atMs; this.regionId = regionId; this.rx = rx; }
    }
    private static final class RowSub {
        final long atMs; final String regionId;
        RowSub(long atMs, String regionId) { this.atMs = atMs; this.regionId = regionId; }
    }
}
