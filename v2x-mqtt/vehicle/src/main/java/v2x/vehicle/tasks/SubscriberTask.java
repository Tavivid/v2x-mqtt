package v2x.vehicle.tasks;

import v2x.vehicle.model.PointCloudChunk;
import v2x.vehicle.net.MasterClient;
import v2x.vehicle.net.RosSubscriber;
import v2x.vehicle.util.PointCloudSerializer;

import java.util.concurrent.TimeUnit;

/**
 * 点群 Subscriber（ROS1 風）。
 *
 * - 指定リージョンの data トピックを購読し
 * - PointCloudChunk に復元してログ出力（将来的に UDP 連携などに差し替え可）
 */
public class SubscriberTask implements Runnable {

    private final MasterClient master;
    private final String regionId;

    public SubscriberTask(MasterClient master, String regionId) {
        this.master = master;
        this.regionId = regionId;
    }

    @Override
    public void run() {
        String topic = "v2x/region/" + regionId + "/data";

        try (RosSubscriber sub = new RosSubscriber(
                master,
                topic,
                payload -> {
                    PointCloudChunk chunk = PointCloudSerializer.deserialize(payload);
                    // とりあえずログに出す（必要に応じて UdpSinkService 連携などに差し替え）
                    System.out.println("[SUB] received chunk region=" + chunk.regionId()
                            + " vehicleId=" + chunk.sourceVehicleId()
                            + " ts=" + chunk.captureTsMillis()
                            + " points=" + (chunk.points() == null ? 0 : chunk.points().size()));
                }
        )) {
            sub.start();
            System.out.println("[SUB] started for region=" + regionId);

            while (!Thread.currentThread().isInterrupted()) {
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            System.err.println("[SUB] error: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[SUB] stopped.");
    }
}
