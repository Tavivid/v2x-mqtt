package v2x.vehicle.ros;

import org.ros.namespace.GraphName;
import org.ros.node.AbstractNodeMain;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Subscriber;
import org.ros.message.MessageListener;

// rosjava の std_msgs
import std_msgs.ByteMultiArray;

import org.jboss.netty.buffer.ChannelBuffer;
import v2x.vehicle.model.Point3D;
import v2x.vehicle.model.PointCloudChunk;
import v2x.vehicle.util.PointCloudSerializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 本物の ROS1 + rosjava 版の Vehicle Subscriber ノード。
 *
 * topic: v2x/region/<regionId>/data (std_msgs/ByteMultiArray)
 *   -> PointCloudChunk に復元してログ＆PCD保存。
 */
public class VehicleSubscriberNode extends AbstractNodeMain {

    private final String regionId;

    public VehicleSubscriberNode(String regionId) {
        this.regionId = regionId;
    }

    @Override
    public GraphName getDefaultNodeName() {
        return GraphName.of("v2x_vehicle_subscriber/" + regionId);
    }

    @Override
    public void onStart(final ConnectedNode connectedNode) {
        String safeRegionId = regionId.replaceAll("[^a-zA-Z0-9_]", "_");
        String topic = "v2x/region/" + safeRegionId + "/data";

        Subscriber<ByteMultiArray> sub =
                connectedNode.newSubscriber(topic, ByteMultiArray._TYPE);

        System.out.println(
                String.format("VehicleSubscriberNode started region=%s topic=%s",
                        regionId, topic));

        sub.addMessageListener(new org.ros.message.MessageListener<ByteMultiArray>() {
            @Override
            public void onNewMessage(ByteMultiArray message) {
                try {
                    // ★ここも生成された Java クラスに依存するので調整が必要
                    //   典型的な rosjava 生成コードなら getData() で byte[] が取れる想定。
                    ChannelBuffer buf = message.getData();
                    byte[] payload = new byte[buf.readableBytes()];
                    buf.getBytes(buf.readerIndex(), payload);

                    PointCloudChunk chunk = PointCloudSerializer.deserialize(payload);

                    System.out.println(
                            String.format(
                                    "[SUB] received chunk region=%s vehicleId=%s ts=%d points=%d",
                                    chunk.regionId(),
                                    chunk.sourceVehicleId(),
                                    chunk.captureTsMillis(),
                                    (chunk.points() == null ? 0 : chunk.points().size())
                            )
                    );

                    saveChunkAsPcd(chunk, connectedNode);
                } catch (Exception e) {
                    System.err.println("[SUB] failed to handle message");
                    e.printStackTrace(System.err);
                }
            }
        });
    }

    /**
     * 旧 SubscriberTask.saveChunkAsPcd() をほぼそのままコピー。
     */
    private static void saveChunkAsPcd(PointCloudChunk chunk,
                                       ConnectedNode connectedNode) {
        List<?> pts = chunk.points();
        if (pts == null || pts.isEmpty()) {
            System.out.println(
                    "[SUB] skip saving empty chunk region=" + chunk.regionId());
            return;
        }

        // ベースディレクトリ: カレントディレクトリ配下の recv-pcd
        File baseDir = new File("/home/tavivid/v2x-pcd/received");
        File regionDir = new File(baseDir, chunk.regionId());
        if (!regionDir.exists() && !regionDir.mkdirs()) {
            System.err.println(
                    "[SUB] failed to create dir: " + regionDir.getAbsolutePath());
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
                new OutputStreamWriter(
                        new FileOutputStream(out),
                        StandardCharsets.US_ASCII))) {

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

            for (Object o : pts) {
                if (!(o instanceof Point3D)) {
                    continue;
                }
                Point3D p = (Point3D) o;
                w.write(p.x() + " " + p.y() + " " + p.z());
                w.newLine();
            }
        } catch (IOException e) {
            System.err.println(
                    "[SUB] failed to save PCD file: " + out.getAbsolutePath());
            e.printStackTrace(System.err);
            return;
        }

        System.out.println(
                "[SUB] saved PCD file: " + out.getAbsolutePath());
    }
}
