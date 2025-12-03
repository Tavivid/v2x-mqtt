package v2x.vehicle;

import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.datasource.PointCloudSource;
import v2x.vehicle.net.MasterClient;
import v2x.vehicle.tasks.PublisherTask;
import v2x.vehicle.tasks.SubscriberTask;

/**
 * ROS1 風 pub/sub の Vehicle ノード。
 *
 * - AppConfig から設定を読み込み
 * - MASTER_HOST / MASTER_PORT から Master の場所を決定
 * - PUB_REGION / SUB_REGION （または -DpubRegion / -DsubRegion）で publish / subscribe リージョンを指定
 */
public class VehicleMain {

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();
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

        // publish / subscribe するリージョン
        String pubRegion = System.getProperty("pubRegion",
                System.getenv().getOrDefault("PUB_REGION", null));
        String subRegion = System.getProperty("subRegion",
                System.getenv().getOrDefault("SUB_REGION", null));

        System.out.println("[Vehicle] id=" + vehicleId
                + " master=" + masterHost + ":" + masterPort
                + " pubRegion=" + pubRegion
                + " subRegion=" + subRegion);

        // Publisher 用の TCP listen ポート
        // 既存の udpSendPort をそのまま流用（vehicleId からオフセット済み）
        int pubPort = cfg.udpSendPort;
        String pubHost = System.getenv().getOrDefault("PUB_HOST", "127.0.0.1");

        // Publisher タスク起動
        if (pubRegion != null && !pubRegion.isBlank()) {
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
                    pubRegion,
                    master,
                    pubHost,
                    pubPort
            );
            Thread pubThread = new Thread(pubTask,
                    "Publisher-" + vehicleId + "-" + pubRegion);
            pubThread.setDaemon(true);
            pubThread.start();
        } else {
            System.out.println("[Vehicle] pubRegion not set -> Publisher disabled.");
        }

        // Subscriber タスク起動
        if (subRegion != null && !subRegion.isBlank()) {
            SubscriberTask subTask = new SubscriberTask(master, subRegion);
            Thread subThread = new Thread(subTask,
                    "Subscriber-" + vehicleId + "-" + subRegion);
            subThread.setDaemon(true);
            subThread.start();
        } else {
            System.out.println("[Vehicle] subRegion not set -> Subscriber disabled.");
        }

        // 単純に終了しないように main スレッドをブロック
        Thread.currentThread().join();
    }
}
