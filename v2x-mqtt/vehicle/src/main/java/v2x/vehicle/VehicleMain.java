package v2x.vehicle;

import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.datasource.PointCloudSource;
import v2x.vehicle.feeder.PublishFeeder;
import v2x.vehicle.feeder.SubscribeFeeder;
import v2x.vehicle.net.MasterClient;
import v2x.vehicle.tasks.PublisherTask;
import v2x.vehicle.tasks.SubscriberTask;

import java.io.File;
import java.net.InetSocketAddress;

/**
 * ROS1 風 pub/sub の Vehicle ノード。
 *
 * モード:
 *   1) 単一リージョン指定:
 *        -DpubRegion / env PUB_REGION
 *        -DsubRegion / env SUB_REGION
 *
 *   2) CSV シナリオ指定:
 *        SUB_FEED_CSV: subscribe.csv (at_ms,region_id,priority,ip,port)
 *        SUB_FEED_LOOP: "1" ならループ
 *
 *        PUB_FEED_CSV: publish.csv (at_ms,region_id,ip,port)
 *        PUB_FEED_LOOP: "1" ならループ
 *
 *   CSV が指定されている場合、その側は CSV 優先で動作し、
 *   PUB_REGION / SUB_REGION 単体指定はその側については無効化される。
 */
public class VehicleMain {

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();
        System.out.println("[DEBUG] datasetPath=" + cfg.datasetPath
            + " datasetGlob=" + cfg.datasetGlob
            + " datasetLoop=" + cfg.datasetLoop);
        String vehicleId = cfg.vehicleId;

        // Master の場所（coordinator 側の MasterServer）
        String masterHost = System.getenv().getOrDefault("MASTER_HOST", "127.0.0.1");
        int masterPort;
        try {
            masterPort = Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "11311"));
        } catch (NumberFormatException e) {
            masterPort = 11311;
        }
        MasterClient master = new MasterClient(masterHost, masterPort);

        // 単一リージョン指定（従来のシンプルモード）
        String pubRegionSingle = System.getProperty(
                "pubRegion",
                System.getenv().getOrDefault("PUB_REGION", null)
        );
        String subRegionSingle = System.getProperty(
                "subRegion",
                System.getenv().getOrDefault("SUB_REGION", null)
        );

        // CSV フィード指定
        String pubCsvPath = System.getenv("PUB_FEED_CSV");
        boolean pubLoop = "1".equals(System.getenv("PUB_FEED_LOOP"));

        String subCsvPath = System.getenv("SUB_FEED_CSV");
        boolean subLoop = "1".equals(System.getenv("SUB_FEED_LOOP"));

        System.out.println("[Vehicle] id=" + vehicleId
                + " master=" + masterHost + ":" + masterPort
                + " pubRegion=" + pubRegionSingle
                + " subRegion=" + subRegionSingle
                + " PUB_FEED_CSV=" + pubCsvPath
                + " SUB_FEED_CSV=" + subCsvPath);

        // Publisher 用の TCP listen ベースポート
        // 既存の udpSendPort をベースに、region ごとに +1 していく
        int basePubPort = cfg.udpSendPort;
        String pubHost = System.getenv().getOrDefault("PUB_HOST", "127.0.0.1");

        // ====== Publisher 側 ======
        if (pubCsvPath != null && !pubCsvPath.isBlank()) {
            // CSV モード優先
            File subCsv = new File(pubCsvPath);
            if (!subCsv.exists()) {
                System.err.println("[PUB] CSV not found: " + subCsv.getAbsolutePath());
            } else {
                Thread feederThread = new Thread(() -> {
                    PublishFeeder feeder = new PublishFeeder(subCsv);
                    do {
                        feeder.run(cfg, master, pubHost, basePubPort);
                        if (!pubLoop) {
                            break;
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ignored) {
                        }
                    } while (true);
                }, "publish-feeder");
                feederThread.setDaemon(true);
                feederThread.start();
                System.out.println("[PUB] csv sequence started file=" + subCsv.getAbsolutePath()
                        + " loop=" + pubLoop);
            }
        } else if (pubRegionSingle != null && !pubRegionSingle.isBlank()) {
            // 単一リージョン指定モード
            PointCloudSource source;
            try {
                source = new DatasetPointCloudSource(cfg.datasetPath, cfg.datasetGlob, cfg.datasetLoop);
            } catch (Exception e) {
                System.err.println("[Vehicle] dataset not available: " + e.getMessage());
                source = null;
            }

            PublisherTask pubTask = new PublisherTask(
                    cfg,
                    source,
                    pubRegionSingle,
                    master,
                    pubHost,
                    basePubPort
            );
            Thread pubThread = new Thread(pubTask,
                    "Publisher-" + vehicleId + "-" + pubRegionSingle);
            pubThread.setDaemon(true);
            pubThread.start();
        } else {
            System.out.println("[Vehicle] pubRegion not set and PUB_FEED_CSV not set -> Publisher disabled.");
        }

        // ====== Subscriber 側 ======
        if (subCsvPath != null && !subCsvPath.isBlank()) {
            // CSV モード優先
            File reqCsv = new File(subCsvPath);
            if (!reqCsv.exists()) {
                System.err.println("[SUB] CSV not found: " + reqCsv.getAbsolutePath());
            } else {
                Thread feederThread = new Thread(() -> {
                    InetSocketAddress defaultRx =
                            new InetSocketAddress("127.0.0.1", cfg.udpRecvPort);
                    SubscribeFeeder feeder = new SubscribeFeeder(reqCsv, defaultRx);
                    do {
                        feeder.run(master);
                        if (!subLoop) {
                            break;
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ignored) {
                        }
                    } while (true);
                }, "subscribe-feeder");
                feederThread.setDaemon(true);
                feederThread.start();
                System.out.println("[SUB] csv sequence started file=" + reqCsv.getAbsolutePath()
                        + " loop=" + subLoop);
            }
        } else if (subRegionSingle != null && !subRegionSingle.isBlank()) {
            // 単一リージョン指定モード
            SubscriberTask subTask = new SubscriberTask(master, subRegionSingle);
            Thread subThread = new Thread(subTask,
                    "Subscriber-" + vehicleId + "-" + subRegionSingle);
            subThread.setDaemon(true);
            subThread.start();
        } else {
            System.out.println("[SUB] subRegion not set and SUB_FEED_CSV not set -> Subscriber disabled.");
        }

        // 終了しないように main スレッドをブロック
        Thread.currentThread().join();
    }
}
