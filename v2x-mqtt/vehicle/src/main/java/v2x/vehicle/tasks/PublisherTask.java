package v2x.vehicle.tasks;

import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.PointCloudSource;
import v2x.vehicle.model.PointCloudChunk;
import v2x.vehicle.net.DedupCache;
import v2x.vehicle.net.MasterClient;
import v2x.vehicle.net.RosPublisher;
import v2x.vehicle.util.PointCloudSerializer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 点群 Publisher（ROS1 風）。
 *
 * - PointCloudSource から PointCloudChunk を取り出し
 * - PointCloudSerializer でシリアライズして
 * - RosPublisher を通じて Subscriber へ送信する
 */
public class PublisherTask implements Runnable {

    private final AppConfig cfg;
    private final String vehicleId;
    private final String regionId;
    private final double publishRateHz;
    private final int maxPointsPerChunk;
    private final PointCloudSource source;

    private final MasterClient master;
    private final RosPublisher rosPublisher;

    private final DedupCache dpd = new DedupCache(2048, Duration.ofSeconds(10));

    public PublisherTask(AppConfig cfg,
                         PointCloudSource source,
                         String regionId,
                         MasterClient master,
                         String advertiseHost,
                         int advertisePort) {
        this.cfg = cfg;
        this.vehicleId = cfg.vehicleId;
        this.regionId = regionId;
        this.publishRateHz = cfg.publishRateHz;
        this.maxPointsPerChunk = cfg.maxPointsPerChunk;
        this.source = source;
        this.master = master;

        String topic = "v2x/region/" + regionId + "/data";
        this.rosPublisher = new RosPublisher(master, topic, advertiseHost, advertisePort);
    }

    @Override
    public void run() {
        try {
            rosPublisher.start();
        } catch (Exception e) {
            System.err.println("[PUB] failed to start RosPublisher: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        long intervalMs = (publishRateHz > 0) ? (long) (1000.0 / publishRateHz) : 500L;
        System.out.println("[PUB] started for region=" + regionId
                + " vehicleId=" + vehicleId
                + " rate=" + publishRateHz + "Hz");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (source == null || publishRateHz <= 0) {
                    TimeUnit.MILLISECONDS.sleep(1000);
                    continue;
                }

                PointCloudChunk chunk = source.nextChunk(vehicleId, regionId, maxPointsPerChunk);
                if (chunk == null) {
                    // データ枯渇
                    TimeUnit.MILLISECONDS.sleep(500);
                    continue;
                }

                String key = chunk.makeDedupKey();
                if (dpd.seen(key, System.currentTimeMillis())) {
                    continue; // 重複チャンクはスキップ
                }

                byte[] payload = PointCloudSerializer.serialize(vehicleId, regionId, chunk);
                rosPublisher.publish(payload);
                TimeUnit.MILLISECONDS.sleep(intervalMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[PUB] error: " + e.getMessage());
                e.printStackTrace();
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        try {
            rosPublisher.close();
        } catch (Exception ignore) {
        }
        System.out.println("[PUB] stopped.");
    }
}
