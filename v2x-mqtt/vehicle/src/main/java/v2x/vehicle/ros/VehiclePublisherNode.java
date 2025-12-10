package v2x.vehicle.ros;

import org.ros.concurrent.CancellableLoop;
import org.ros.namespace.GraphName;
import org.ros.node.AbstractNodeMain;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Publisher;

// rosjava の std_msgs (メッセージ生成済みを想定)
import std_msgs.ByteMultiArray;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import java.nio.ByteOrder;

import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.PointCloudSource;
import v2x.vehicle.model.PointCloudChunk;
import v2x.vehicle.util.PointCloudSerializer;

/**
 * 本物の ROS1 + rosjava 版の Vehicle Publisher ノード。
 *
 * 旧 PublisherTask とほぼ同じ処理を rosjava の CancellableLoop 上で行う。
 *
 * topic: v2x/region/<regionId>/data  (std_msgs/ByteMultiArray)
 */
public class VehiclePublisherNode extends AbstractNodeMain {

    private final AppConfig cfg;
    private final String vehicleId;
    private final String regionId;
    private final PointCloudSource source;

    public VehiclePublisherNode(AppConfig cfg,
                                PointCloudSource source,
                                String regionId) {
        this.cfg = cfg;
        this.vehicleId = cfg.vehicleId;
        this.regionId = regionId;
        this.source = source;
    }

    @Override
    public GraphName getDefaultNodeName() {
        // ノード名は適当に
        return GraphName.of("v2x_vehicle_publisher/" + vehicleId + "/" + regionId);
    }

    @Override
    public void onStart(final ConnectedNode connectedNode) {
        final double publishRateHz = cfg.publishRateHz;
        final int maxPointsPerChunk = cfg.maxPointsPerChunk;

        final Publisher<ByteMultiArray> publisher =
                connectedNode.newPublisher(
                        "v2x/region/" + regionId + "/data",
                        ByteMultiArray._TYPE
                );

        connectedNode.getLog().info(
                String.format(
                        "VehiclePublisherNode started vehicleId=%s region=%s rate=%.3fHz",
                        vehicleId, regionId, publishRateHz));

        connectedNode.executeCancellableLoop(new CancellableLoop() {
            private long intervalMs;

            @Override
            protected void setup() {
                intervalMs = (publishRateHz > 0.0)
                        ? (long) (1000.0 / publishRateHz)
                        : 500L;
            }

            @Override
            protected void loop() throws InterruptedException {
                try {
                    if (source == null || publishRateHz <= 0.0) {
                        Thread.sleep(1000L);
                        return;
                    }

                    PointCloudChunk chunk =
                            source.nextChunk(vehicleId, regionId, maxPointsPerChunk);

                    if (chunk == null) {
                        // データ枯渇
                        Thread.sleep(500L);
                        return;
                    }

                    byte[] payload =
                            PointCloudSerializer.serialize(vehicleId, regionId, chunk);

                    ByteMultiArray msg = publisher.newMessage();
                    ChannelBuffer buf = ChannelBuffers.copiedBuffer(ByteOrder.LITTLE_ENDIAN, payload);
                    msg.setData(buf);
                    publisher.publish(msg);

                    connectedNode.getLog().info(
                            String.format(
                                    "[PUB] sent chunk region=%s vehicleId=%s ts=%d points=%d",
                                    chunk.regionId(),
                                    chunk.sourceVehicleId(),
                                    chunk.captureTsMillis(),
                                    (chunk.points() == null ? 0 : chunk.points().size())
                            )
                    );

                    Thread.sleep(intervalMs);
                } catch (Exception e) {
                    connectedNode.getLog().error("[PUB] error", e);
                    Thread.sleep(1000L);
                }
            }
        });
    }
}
