package org.example.v2x.vehicle;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.example.v2x.common.config.AppConfig;
import org.example.v2x.common.geo.GeoHash;
import org.example.v2x.vehicle.datasource.DatasetPointCloudSource;
import org.example.v2x.vehicle.datasource.PointCloudSource;
import org.example.v2x.vehicle.net.MqttClientFactory;
import org.example.v2x.vehicle.tasks.PublisherTask;
import org.example.v2x.vehicle.tasks.RequesterTask;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 現行仕様の Vehicle エントリポイント（CSVフィード維持版）
 *
 * - /data は RequesterTask をリスナーとして購読（fetch-request受信→UDP直送は RequesterTask 側）
 * - /request の発行は CSV（REQ_FEED_CSV）でスケジュール
 * - 動的購読は CSV（SUB_FEED_CSV）で制御（一定時間(SUB_IDLE_MS)で自動解除）
 *
 * 環境変数:
 *   REQ_FEED_CSV   ... /request 送信用CSV (at_ms,region_id,ip,port)
 *   REQ_FEED_LOOP  ... "1" ならループ
 *   SUB_FEED_CSV   ... 動的購読用CSV (at_ms,region_id)
 *   SUB_FEED_LOOP  ... "1" ならループ
 *   SUB_IDLE_MS    ... 動的購読のアイドル解除ミリ秒（既定 5000）
 */
public class VehicleMain {

    // ================= 動的購読マネージャ =================
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
                    client.subscribe(topic, /*qos*/1, listener);
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
    // =====================================================

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();

        // （購読の初期値。送信には使わない）
        String region = System.getProperty("region", null);
        if (region == null || region.isBlank()) {
            // 例: 東京駅付近
            region = GeoHash.encode(35.681236, 139.767125, cfg.geohashPrecision);
        }

        // MQTT 接続（ClientID は Factory 側でユニーク化推奨）
        MqttClient client = MqttClientFactory.connect(cfg.mqttHost, cfg.mqttPort, cfg.mqttClientPrefix + cfg.vehicleId);

        // Publisher（データセットがあれば起動／無ければ起動しない）
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

        // === /data 受信（fetch-request）: RequesterTask をリスナーとして利用 ===
        RequesterTask reqHandler = new RequesterTask(cfg);
        IMqttMessageListener listener = reqHandler.asListener();

        // 方針1: 全リージョン一括購読（シンプル）
        client.subscribe("v2x/region/+/data", 1, listener);

        // 追加で、動的購読（任意・SUB_FEED_CSVが与えられたときだけ）
        long idleMs = Long.parseLong(System.getenv().getOrDefault("SUB_IDLE_MS", "50000"));
        DynamicSubscriptionManager dynSub = new DynamicSubscriptionManager(client, idleMs, listener);
        dynSub.start();

        System.out.println("Vehicle started (CSV feeders enabled; /data subscribed with RequesterTask) defaultRegion=" + region + ", dataset=" + cfg.datasetPath);

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
                        runRequestFeeder(reqCsv, client/*, dynSub*/); // ← ここでは購読に紐付けず送信のみ
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
                        runSubscribeFeeder(subCsv, dynSub); // touch(region) のみ。送信はしない
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

        // 設定ファイルの存在確認（Shade/Jarに埋め込む前提）
        try (InputStream in = VehicleMain.class.getResourceAsStream("/v2x-config.yml")) {
            if (in == null) {
                System.err.println("WARNING: v2x-config.yml not found in classpath!");
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(dynSub::close));
        Thread.currentThread().join();
    }

    // ======= /request 発行フィーダ（CSV） =======
    private static void runRequestFeeder(File csv, MqttClient client/*, DynamicSubscriptionManager dynSub*/) {
        try {
            List<RowReq> rows = loadReqCsv(csv);
            if (rows.isEmpty()) { System.out.println("[FEED-REQ] no rows."); return; }
            long start = System.currentTimeMillis();
            ObjectMapper mapper = new ObjectMapper();

            for (RowReq r : rows) {
                long due = start + r.atMs;
                long now = System.currentTimeMillis();
                if (due > now) TimeUnit.MILLISECONDS.sleep(due - now);

                // （以前はここで dynSub.touch(r.regionId) していたが、要求→購読の結合をやめるため削除）
                // dynSub.touch(r.regionId);

                Map<String, Object> body = new LinkedHashMap<>();
                body.put("type", "need-pointcloud");
                body.put("region_id", r.regionId);
                body.put("rx_udp", Map.of("ip", r.rx.getAddress().getHostAddress(), "port", r.rx.getPort()));
                body.put("ts_ms", System.currentTimeMillis());
                body.put("nonce", UUID.randomUUID().toString());

                byte[] payload = mapper.writeValueAsBytes(body);
                MqttMessage msg = new MqttMessage(payload);
                msg.setQos(1);          // 取りこぼし低減のため QoS=1
                msg.setRetained(false);

                String topic = "v2x/region/" + r.regionId + "/request";
                client.publish(topic, msg);
                System.out.printf("[FEED-REQ] published topic=%s payload=%s%n",
                        topic, new String(payload, StandardCharsets.UTF_8));
            }
            System.out.println("[FEED-REQ] sequence done");
        } catch (Exception e) {
            System.err.println("[FEED-REQ] error: " + e);
        }
    }

    // ======= 動的購読フィーダ（CSV） =======
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

    // ======= CSV 行モデル =======
    private static final class RowReq {
        final long atMs; final String regionId; final InetSocketAddress rx;
        RowReq(long atMs, String regionId, InetSocketAddress rx) { this.atMs = atMs; this.regionId = regionId; this.rx = rx; }
    }
    private static final class RowSub {
        final long atMs; final String regionId;
        RowSub(long atMs, String regionId) { this.atMs = atMs; this.regionId = regionId; }
    }
}
