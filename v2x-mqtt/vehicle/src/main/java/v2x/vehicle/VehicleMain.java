package v2x.vehicle;

import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.datasource.PointCloudSource;
import v2x.vehicle.feeder.PublishFeeder;
import v2x.vehicle.feeder.RegionTimelineFeeder;
import v2x.vehicle.feeder.SubscribeFeeder;
import v2x.vehicle.feeder.RegionTimelineSubscriberFeeder;
import v2x.vehicle.net.MasterClient;
import v2x.vehicle.tasks.PublisherTask;
import v2x.vehicle.tasks.SubscriberTask;

import java.io.File;
import java.io.PrintStream;
import java.net.InetSocketAddress;

/**
 * ROS1 風 pub/sub の Vehicle ノード。
 *
 * モード:
 * 1) 単一リージョン指定:
 *    -DpubRegion / env PUB_REGION
 *    -DsubRegion / env SUB_REGION
 *
 * 2) CSV シナリオ指定:
 *    SUB_FEED_CSV: subscribe.csv (at_ms,region_id,priority,ip,port)
 *    SUB_FEED_LOOP: "1" ならループ
 *
 *    PUB_FEED_CSV: publish.csv (at_ms,region_id,ip,port)
 *    PUB_FEED_LOOP: "1" ならループ
 *
 * 3) JSON タイムライン指定（今回追加）:
 *    REGION_TIMELINE_DIR: フレームごとの JSON が並ぶディレクトリ
 *                         省略時は cfg.datasetPath を使用
 *    REGION_TIMELINE_STEP_MS: フレーム間隔(ms) 例: 100 -> 10Hz
 *
 * CSV / JSON が指定されている場合、その側はシナリオ優先で動作し、
 * PUB_REGION / SUB_REGION 単体指定はその側については無効化される。
 */
public class VehicleMain {
    private static volatile boolean syncDone = false;

    public static void main(String[] args) throws Exception {
        installTimestampedLogger();
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

        // JSON タイムライン指定（新規）
        String regionTimelineDirEnv = System.getenv("REGION_TIMELINE_DIR");
        String regionTimelineStepMsEnv = System.getenv("REGION_TIMELINE_STEP_MS");
        long regionTimelineStepMs = 100L; // デフォルト 100ms = 10Hz
        if (regionTimelineStepMsEnv != null && !regionTimelineStepMsEnv.isBlank()) {
            try {
                regionTimelineStepMs = Long.parseLong(regionTimelineStepMsEnv.trim());
            } catch (NumberFormatException ignored) {
            }
        }

        System.out.println("[Vehicle] id=" + vehicleId
                + " master=" + masterHost + ":" + masterPort
                + " pubRegion=" + pubRegionSingle
                + " subRegion=" + subRegionSingle
                + " PUB_FEED_CSV=" + pubCsvPath
                + " SUB_FEED_CSV=" + subCsvPath
                + " REGION_TIMELINE_DIR=" + regionTimelineDirEnv
                + " REGION_TIMELINE_STEP_MS=" + regionTimelineStepMs);

        // Publisher 用の TCP listen ベースポート
        // 既存の udpSendPort をベースに、region ごとに +1 していく
        int basePubPort = cfg.udpSendPort;
        String pubHost = System.getenv().getOrDefault("PUB_HOST", "127.0.0.1");

        waitSyncStartIfConfigured();

        // ====== Publisher 側 ======
        // タイムラインモード:
        //   REGION_TIMELINE_DIR  ... JSON が並んでいるディレクトリ
        //                            未指定なら cfg.datasetPath を使う
        //   REGION_TIMELINE_STEP_MS ... フレーム間隔 (省略時 100ms)
        //   REGION_TIMELINE_LOOP ... "1" なら最後まで行ったら先頭に戻る
        String regionTimelineDirPath = System.getenv("REGION_TIMELINE_DIR");
        long timelineStepMs;
        try {
            timelineStepMs = Long.parseLong(System.getenv().getOrDefault("REGION_TIMELINE_STEP_MS", "100"));
        } catch (NumberFormatException e) {
            timelineStepMs = 100L;
        }
        boolean timelineLoop = "1".equals(System.getenv("REGION_TIMELINE_LOOP"));

        File tdir = null;
        if (regionTimelineDirPath != null && !regionTimelineDirPath.isBlank()) {
            tdir = new File(regionTimelineDirPath);
        }

        if (tdir != null && tdir.isDirectory()) {
            RegionTimelineFeeder feeder =
                    new RegionTimelineFeeder(cfg, master, vehicleId, pubHost, basePubPort, tdir, timelineStepMs, timelineLoop);
            Thread feederThread = new Thread(feeder, "region-timeline-feeder");
            feederThread.setDaemon(true);
            feederThread.start();
            System.out.println("[PUB] region timeline started dir=" + tdir.getAbsolutePath()
                    + " stepMs=" + timelineStepMs + " loop=" + timelineLoop);
        } else if (tdir != null && !tdir.isDirectory()) {
            System.err.println("[PUB] timeline dir is not directory: " + tdir.getAbsolutePath());
        } else if (pubCsvPath != null && !pubCsvPath.isBlank()) {
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
                System.out.println("[PUB] csv sequence started file=" + subCsv.getAbsolutePath() + " loop=" + pubLoop);
            }
        } else if (pubRegionSingle != null && !pubRegionSingle.isBlank()) {
            // 単一リージョン指定モード（従来どおり publish_rate_hz を使う簡易デモ）
            PointCloudSource source;
            try {
                source = new DatasetPointCloudSource(cfg.datasetPath, cfg.datasetGlob, cfg.datasetLoop);
            } catch (Exception e) {
                System.err.println("[Vehicle] dataset not available: " + e.getMessage());
                source = null;
            }
            PublisherTask pubTask = new PublisherTask(
                    cfg, source, pubRegionSingle, master, pubHost, basePubPort
            );
            Thread pubThread = new Thread(pubTask, "Publisher-" + vehicleId + "-" + pubRegionSingle);
            pubThread.setDaemon(true);
            pubThread.start();
        } else {
            System.out.println("[Vehicle] pubRegion not set and PUB_FEED_CSV not set and REGION_TIMELINE_DIR/datasetPath not set -> Publisher disabled.");
        }

        // ====== Subscriber 側 ======
        // タイムライン購読モード:
        //   SUB_TIMELINE_DIR       ... 「欲しい領域一覧 JSON」が並んでいるディレクトリ
        //   SUB_TIMELINE_STEP_MS   ... フレーム間隔 (省略時 100ms = 10Hz)
        //   SUB_TIMELINE_LOOP      ... "1" なら最後まで行ったら先頭に戻る
        String subTimelineDirPath = System.getenv("SUB_TIMELINE_DIR");
        long subTimelineStepMs;
        try {
            subTimelineStepMs = Long.parseLong(
                    System.getenv().getOrDefault("SUB_TIMELINE_STEP_MS", "100")
            );
        } catch (NumberFormatException e) {
            subTimelineStepMs = 10000L;
        }
        boolean subTimelineLoop = "1".equals(System.getenv("SUB_TIMELINE_LOOP"));

        if (subTimelineDirPath != null && !subTimelineDirPath.isBlank()) {
            // ★ JSON タイムラインモード
            File subTdir = new File(subTimelineDirPath);
            if (!subTdir.isDirectory()) {
                System.err.println("[SUB] SUB_TIMELINE_DIR is not directory: " + subTdir.getAbsolutePath());
            } else {
                RegionTimelineSubscriberFeeder feeder =
                        new RegionTimelineSubscriberFeeder(master, subTdir, subTimelineStepMs, subTimelineLoop);
                Thread feederThread = new Thread(feeder, "region-timeline-subscriber");
                feederThread.setDaemon(true);
                feederThread.start();
                System.out.println("[SUB] region timeline started dir=" + subTdir.getAbsolutePath()
                        + " stepMs=" + subTimelineStepMs + " loop=" + subTimelineLoop);
            }

        } else if (subCsvPath != null && !subCsvPath.isBlank()) {
            // 旧来の CSV モード
            File reqCsv = new File(subCsvPath);
            if (!reqCsv.exists()) {
                System.err.println("[SUB] CSV not found: " + reqCsv.getAbsolutePath());
            } else {
                Thread feederThread = new Thread(() -> {
                    InetSocketAddress defaultRx = new InetSocketAddress("127.0.0.1", cfg.udpRecvPort);
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
                System.out.println("[SUB] csv sequence started file=" + reqCsv.getAbsolutePath() + " loop=" + subLoop);
            }

        } else if (subRegionSingle != null && !subRegionSingle.isBlank()) {
            // 単一リージョン固定モード（昔のモード）
            SubscriberTask subTask = new SubscriberTask(master, subRegionSingle);
            Thread subThread = new Thread(subTask, "Subscriber-" + vehicleId + "-" + subRegionSingle);
            subThread.setDaemon(true);
            subThread.start();

        } else {
            System.out.println("[SUB] subRegion not set and SUB_FEED_CSV/SUB_TIMELINE_DIR not set -> Subscriber disabled.");
        }

        // 終了しないように main スレッドをブロック
        Thread.currentThread().join();
    }

    // ====== 同期開始バリア (SYNC_START_AT_SEC が指定されていたら、その時刻まで待つ) ======
    private static void waitSyncStartIfConfigured() {
        String secStr = System.getenv("SYNC_START_AT_SEC");
        if (secStr == null || secStr.isBlank()) {
            return; // 指定なし → 何もしない
        }
        try {
            long targetSec = Long.parseLong(secStr.trim());
            while (true) {
                long nowSec = System.currentTimeMillis() / 1000L;
                long diff = targetSec - nowSec;
                if (diff <= 0) {
                    break;
                }
                long sleepMs = Math.min(diff * 1000L, 500L);
                Thread.sleep(sleepMs);
            }
            System.out.println("[SYNC] started at " + System.currentTimeMillis() / 1000L
                    + " (target=" + targetSec + ")");
        } catch (NumberFormatException e) {
            System.err.println("[SYNC] invalid SYNC_START_AT_SEC: " + secStr);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void installTimestampedLogger() {
        PrintStream baseOut = System.out;
        System.setOut(new PrintStream(baseOut) {
            @Override
            public void println(String x) {
                super.println("[" + nowTs() + "] " + x);
            }

            @Override
            public void println(Object x) {
                super.println("[" + nowTs() + "] " + String.valueOf(x));
            }
        });

        PrintStream baseErr = System.err;
        System.setErr(new PrintStream(baseErr) {
            @Override
            public void println(String x) {
                super.println("[" + nowTs() + "] " + x);
            }

            @Override
            public void println(Object x) {
                super.println("[" + nowTs() + "] " + String.valueOf(x));
            }
        });
    }

    private static String nowTs() {
        // 例: 2025-12-09T03:21:45.123
        return java.time.LocalDateTime.now().toString();
    }
}
