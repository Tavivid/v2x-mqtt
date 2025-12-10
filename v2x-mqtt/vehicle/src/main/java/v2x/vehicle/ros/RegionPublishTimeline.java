/*
package v2x.vehicle.ros;

import org.ros.concurrent.CancellableLoop;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Publisher;
import std_msgs.ByteMultiArray;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.File;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

public class RegionPublishTimeline extends CancellableLoop {

    private final ConnectedNode node;
    private final String vehicleId;
    private final File regionTimelineDir;

    // regionId → Publisher
    private final Map<String, Publisher<ByteMultiArray>> active = new HashMap<>();

    public RegionPublishTimeline(ConnectedNode node, String vehicleId, String regionTimelineDir) {
        this.node = node;
        this.vehicleId = vehicleId;
        this.regionTimelineDir = new File(regionTimelineDir);
    }

    @Override
    protected void setup() throws Exception {
        // ここに、今 VehicleTimelineNode.PublisherTimelineLoop の
        // setup() に書いてあった「タイムラインJSON読み込み」「最初の状態セット」などをほぼ移植
    }

    @Override
    protected void loop() throws Exception {
        // ここに PublisherTimelineLoop.loop() の中身をほぼコピペで移す
        // 例: JSON の次のイベントを見て、Publisher を作ったり削除したり、publish したり
    }

    private Publisher<ByteMultiArray> getOrCreatePublisher(String regionId) {
        String safeRegionId = regionId.replaceAll("[^a-zA-Z0-9_]", "_");
        String topic = "v2x/region/" + safeRegionId + "/data";

        Publisher<ByteMultiArray> pub = active.get(regionId);
        if (pub == null) {
            pub = node.newPublisher(topic, ByteMultiArray._TYPE);
            active.put(regionId, pub);
        }
        return pub;
    }

    private void publishPayload(String regionId, byte[] payload) {
        Publisher<ByteMultiArray> pub = getOrCreatePublisher(regionId);
        ByteMultiArray msg = pub.newMessage();

        ChannelBuffer buf = ChannelBuffers.copiedBuffer(ByteOrder.LITTLE_ENDIAN, payload);
        msg.setData(buf);
        pub.publish(msg);
    }
}
*/