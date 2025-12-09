package v2x.vehicle.tasks;

import v2x.vehicle.model.Point3D;
import v2x.vehicle.model.PointCloudChunk;
import v2x.vehicle.net.MasterClient;
import v2x.vehicle.net.RosSubscriber;
import v2x.vehicle.util.PointCloudSerializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 点群 Subscriber（ROS1 風）。
 *
 * - 指定リージョンの data トピックを購読し
 * - PointCloudChunk に復元してログ出力
 * - 受信した点群を PCD(ASCII) ファイルとして保存する
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
                    // 既存のログ出力
                    System.out.println("[SUB] received chunk region=" + chunk.regionId()
                            + " vehicleId=" + chunk.sourceVehicleId()
                            + " ts=" + chunk.captureTsMillis()
                            + " points=" + (chunk.points() == null ? 0 : chunk.points().size()));

                    // 受信した点群を PCD として保存
                    saveChunkAsPcd(chunk);
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

    /**
     * 受信したチャンクを PCD(ASCII) ファイルとして保存する。
     *
     * 出力先: ./recv-pcd/<regionId>/<captureTsMillis>.pcd
     */
    private static void saveChunkAsPcd(PointCloudChunk chunk) {
        List<?> pts = chunk.points();
        if (pts == null || pts.isEmpty()) {
            System.out.println("[SUB] skip saving empty chunk region=" + chunk.regionId());
            return;
        }

        // ベースディレクトリ: カレントディレクトリ配下の recv-pcd
        File baseDir = new File("/home/tavivid/v2x-pcd/received");
        File regionDir = new File(baseDir, chunk.regionId());
        if (!regionDir.exists() && !regionDir.mkdirs()) {
            System.err.println("[SUB] failed to create dir: " + regionDir.getAbsolutePath());
            return;
        }

        String fileName;
        if (chunk.sourceFileName() != null && !chunk.sourceFileName().isBlank()) {
            // 送信元が持っていたファイル名をそのまま使う
            fileName = chunk.sourceFileName();
        } else {
            // 古い Publisher 互換用のフォールバック
            fileName = chunk.captureTsMillis() + ".pcd";
        }
        File out = new File(regionDir, fileName);

        int numPoints = pts.size();

        try (BufferedWriter w = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.US_ASCII))) {

            // PCD ヘッダ (x,y,z のみ, float32, ascii)
            w.write("# .PCD v0.7\n");
            w.write("VERSION 0.7\n");
            w.write("FIELDS x y z\n");
            w.write("SIZE 4 4 4\n");
            w.write("TYPE F F F\n");
            w.write("COUNT 1 1 1\n");
            w.write("WIDTH " + numPoints + "\n");
            w.write("HEIGHT 1\n");
            w.write("VIEWPOINT 0 0 0 1 0 0 0\n");
            w.write("POINTS " + numPoints + "\n");
            w.write("DATA ascii\n");

            // 点データ本体
            for (Object o : pts) {
                if (!(o instanceof Point3D)) {
                    continue; // 想定外の型はスキップ
                }
                Point3D p = (Point3D) o;
                w.write(p.x() + " " + p.y() + " " + p.z());
                w.newLine();
            }
        } catch (IOException e) {
            System.err.println("[SUB] failed to save PCD file: " + out.getAbsolutePath());
            e.printStackTrace();
            return;
        }

        System.out.println("[SUB] saved PCD file: " + out.getAbsolutePath());
    }
}
