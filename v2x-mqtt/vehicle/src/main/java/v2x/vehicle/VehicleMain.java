package v2x.vehicle;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.paho.client.mqttv3.*;
import v2x.vehicle.config.AppConfig;
import v2x.vehicle.geo.GeoHash;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.datasource.PointCloudSource;
import v2x.vehicle.net.MqttClientFactory;
import v2x.vehicle.publish.PreconnectManager;
import v2x.vehicle.feeder.SubscribeFeeder;
import v2x.vehicle.tasks.PublisherTask;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import v2x.vehicle.feeder.PublishFeeder;

public class VehicleMain {

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();

        String region = System.getProperty("region", null);

        // MQTT 接続（ClientID は Factory 側でユニーク化推奨）
        MqttClient client = MqttClientFactory.connect(cfg.mqttHost, cfg.mqttPort, cfg.mqttClientPrefix + cfg.vehicleId);

        v2x.vehicle.net.UdpSinkService udpSink = new v2x.vehicle.net.UdpSinkService(cfg);
        udpSink.start();

        // Publisher（データセットがあれば起動／無ければ起動しない）
        PointCloudSource source = null;
        boolean startPublisher = true;
        try {
            source = new DatasetPointCloudSource(cfg.datasetPath, cfg.datasetGlob, cfg.datasetLoop);
        } catch (Exception e) {
            System.err.println("[Vehicle] Dataset not available: " + e.getMessage());
            startPublisher = false;
        }

        PublisherTask pubHandler = new PublisherTask(
            client,
            cfg.vehicleId,
            /* regionId */ region,
            /* publishRateHz */ startPublisher ? cfg.publishRateHz : 0.0, // 0 で publish 停止
            cfg.maxPointsPerChunk,
            /* source */ startPublisher ? source : null,
            cfg
        );

        new Thread(pubHandler, "Publisher").start();
        IMqttMessageListener listener = pubHandler.asListener();

        // SUB_FEED_CSVを見て発行可能領域のトピックに接続
        long idleMs = Long.parseLong(System.getenv().getOrDefault("SUB_IDLE_MS", "50000"));
        PreconnectManager prec = new PreconnectManager(client, idleMs, listener);
        prec.start();

        System.out.println("Vehicle started (CSV feeders enabled; /data subscribed with RequesterTask) defaultRegion=" + region + ", dataset=" + cfg.datasetPath);

        // ====== 購読用CSV（REQ_FEED_CSV）: at_ms,region_id,ip,port ======
        String reqCsvPath = System.getenv("SUB_FEED_CSV");
        boolean reqLoop = "1".equals(System.getenv("SUB_FEED_LOOP"));
        if (reqCsvPath != null && !reqCsvPath.isBlank()) {
            File reqCsv = new File(reqCsvPath);
            if (!reqCsv.exists()) {
                System.err.println("[SUB] CSV not found: " + reqCsv.getAbsolutePath());
            } else {
                Thread reqFeeder = new Thread(() -> {
                    java.net.InetSocketAddress defaultRx = new java.net.InetSocketAddress("127.0.0.1", cfg.udpRecvPort);
                    SubscribeFeeder sfeeder = new SubscribeFeeder(reqCsv, defaultRx);
                    //SubscribeFeeder sfeeder = new SubscribeFeeder(reqCsv);
                    do {
                        sfeeder.run(client);
                        if (!reqLoop) {
                            break;
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ignored) {
                        }
                    } while (true);
                }, "request-feeder");
                reqFeeder.setDaemon(true);
                reqFeeder.start();
                System.out.println("[SUB] csv sequence started file=" + reqCsv.getAbsolutePath() + " loop=" + reqLoop);
            }
        } else {
            System.out.println("[SUB] SUB_FEED_CSV not set -> no /request will be sended.");
        }

        // ====== 発行用CSV（SUB_FEED_CSV）: at_ms,region_id ======
        String subCsvPath = System.getenv("PUB_FEED_CSV");
        boolean subLoop = "1".equals(System.getenv("PUB_FEED_LOOP"));
        if (subCsvPath != null && !subCsvPath.isBlank()) {
            File subCsv = new File(subCsvPath);
            if (!subCsv.exists()) {
                System.err.println("[PUB] CSV not found: " + subCsv.getAbsolutePath());
            } else {
                Thread subFeeder = new Thread(() -> {
                    PublishFeeder pfeeder = new PublishFeeder(subCsv);
                    do {
                        pfeeder.run(prec);
                        if (!subLoop) {
                            break;
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ignored) {
                        }
                    } while (true);
                }, "subscribe-feeder");
                subFeeder.setDaemon(true);
                subFeeder.start();
                System.out.println("[PUB] csv sequence started file=" + subCsv.getAbsolutePath() + " loop=" + subLoop);
            }
        } else {
            System.out.println("[PUB] PUB_FEED_CSV not set -> no /data preconnection will be added.");
        }

        // 設定ファイルの存在確認（Shade/Jarに埋め込む前提）
        try (InputStream in = VehicleMain.class.getResourceAsStream("/v2x-config.yml")) {
            if (in == null) {
                System.err.println("WARNING: v2x-config.yml not found in classpath!");
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                udpSink.close();
            } catch (Exception ignore) {
            }
        }));
        Runtime.getRuntime().addShutdownHook(new Thread(prec::close));
        Thread.currentThread().join();
    }
}
