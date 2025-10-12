package org.example.v2x.vehicle;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class VehicleMain {

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();

        // 位置情報があるならここで緯度経度からGeoHashを生成（デモでは固定）
        String region = System.getProperty("region", null);
        if (region == null || region.isBlank()) {
            // 例: 東京駅付近
            region = GeoHash.encode(35.681236, 139.767125, cfg.geohashPrecision);
        }

        MqttClient client = MqttClientFactory.connect(cfg.mqttHost, cfg.mqttPort, cfg.mqttClientPrefix + cfg.vehicleId);

        // データセットソースの準備（★存在しない・読み込めないときは Publisher を起動しない）
        PointCloudSource source = null;
        boolean startPublisher = true;
        try {
            source = new DatasetPointCloudSource(cfg.datasetPath, cfg.datasetGlob, cfg.datasetLoop);
        } catch (Exception e) {
            System.err.println("[Vehicle] Dataset not available: " + e.getMessage());
            startPublisher = false;
        }

        // パブリッシャ（データセット由来の点群発行）※今はデータセット無しなら起動しない
        if (startPublisher && source != null) {
            new Thread(new PublisherTask(client, cfg.vehicleId, region, cfg.publishRateHz, cfg.maxPointsPerChunk, source),
                    "Publisher").start();
        } else {
            System.out.println("[Vehicle] No dataset -> PublisherTask is not started.");
        }

        // リクエスタ（必要なリージョンのデータ要求）
        final var regionFinal = region;
        final var clientFinal = client;

        new Thread(() -> {
            try {
                new RequesterTask(clientFinal, regionFinal).run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "Requester").start();

        System.out.println("Vehicle started for region=" + region + ", dataset=" + cfg.datasetPath);

        // ★★★ 追記: CSVフィーダ（仮の要求リージョンを読み取り、/request を投げる）★★★
        // 環境変数:
        //   REQ_FEED_CSV=./requests.csv     # CSVのパス（at_ms,region_id,ip,port）
        //   REQ_FEED_LOOP=1                 # 1なら終わったらループ
        String feedPath = System.getenv("REQ_FEED_CSV");
        boolean feedLoop = "1".equals(System.getenv("REQ_FEED_LOOP"));
        if (feedPath != null && !feedPath.isBlank()) {
            File csv = new File(feedPath);
            if (!csv.exists()) {
                System.err.println("[FEED] CSV not found: " + csv.getAbsolutePath());
            } else {
                Thread feeder = new Thread(() -> runCsvFeeder(csv, client), "region-request-feeder");
                feeder.setDaemon(true);
                feeder.start();
                System.out.println("[FEED] started with file=" + csv.getAbsolutePath() + " loop=" + feedLoop);

                if (feedLoop) {
                    // ループ駆動：シンプルに別スレッドでもう一段ラッパを回す
                    Thread loopThread = new Thread(() -> {
                        try {
                            while (true) {
                                feeder.join(); // 1巡終わるまで待つ
                                Thread.sleep(100); // ごく短いインターバル
                                Thread again = new Thread(() -> runCsvFeeder(csv, client), "region-request-feeder");
                                again.setDaemon(true);
                                again.start();
                                // 次の周回用に置き換え
                                synchronized (VehicleMain.class) {
                                    // no shared state
                                }
                                // 新しいスレッドを次の join 対象にするため置き換え
                                // ただしここでは単純化のため再代入せず再びjoinする
                                feeder.join(1); // ダミー
                            }
                        } catch (InterruptedException ignored) {}
                    }, "region-request-feeder-loop");
                    loopThread.setDaemon(true);
                    loopThread.start();
                }
            }
        }

        // 設定ファイルの存在確認（Shade/Jarに埋め込む前提）
        try (InputStream in = VehicleMain.class.getResourceAsStream("/v2x-config.yml")) {
            if (in == null) {
                System.err.println("WARNING: v2x-config.yml not found in classpath!");
            }
        }
    }

    /**
     * CSV を読み、指定タイミングで /request を publish する。
     * CSVフォーマット:
     *   # at_ms,region_id,ip,port
     *   0,cell-12,127.0.0.1,51235
     *   800,cell-13,127.0.0.1,51235
     */
    private static void runCsvFeeder(File csv, MqttClient client) {
        try {
            List<Row> rows = loadCsv(csv);
            if (rows.isEmpty()) {
                System.out.println("[FEED] no rows. nothing to do.");
                return;
            }
            long start = System.currentTimeMillis();
            for (Row r : rows) {
                long due = start + r.atMs;
                long now = System.currentTimeMillis();
                if (due > now) {
                    TimeUnit.MILLISECONDS.sleep(due - now);
                }
                publishRequest(client, r.regionId, r.rx);
            }
            System.out.println("[FEED] sequence done");
        } catch (Exception e) {
            System.err.println("[FEED] error: " + e);
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
            System.out.printf("[FEED] published topic=%s payload=%s%n",
                    topic, new String(payload, StandardCharsets.UTF_8));
        } catch (Exception e) {
            System.err.println("[FEED] publish failed: " + e);
        }
    }

    private static List<Row> loadCsv(File file) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            List<Row> out = new ArrayList<>();
            String line;
            long lineno = 0;
            while ((line = br.readLine()) != null) {
                lineno++;
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] tk = line.split(",", -1);
                if (tk.length < 4) {
                    throw new IllegalArgumentException("CSV format error at line " + lineno +
                            " (expect: at_ms,region_id,ip,port)");
                }
                long at = Long.parseLong(tk[0].trim());
                String regionId = tk[1].trim();
                String ip = tk[2].trim();
                int port = Integer.parseInt(tk[3].trim());
                out.add(new Row(at, regionId, new InetSocketAddress(ip, port)));
            }
            // at_ms でソート
            return out.stream()
                    .sorted(Comparator.comparingLong(r -> r.atMs))
                    .collect(Collectors.toList());
        }
    }

    private static final class Row {
        final long atMs;
        final String regionId;
        final InetSocketAddress rx;
        Row(long atMs, String regionId, InetSocketAddress rx) {
            this.atMs = atMs;
            this.regionId = regionId;
            this.rx = rx;
        }
    }
}
