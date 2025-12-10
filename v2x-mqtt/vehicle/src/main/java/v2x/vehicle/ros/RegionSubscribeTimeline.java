/*
package v2x.vehicle.ros;

import org.ros.concurrent.CancellableLoop;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Subscriber;
import org.ros.message.MessageListener;
import org.jboss.netty.buffer.ChannelBuffer;
import std_msgs.ByteMultiArray;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class RegionSubscribeTimeline extends CancellableLoop {

    private final ConnectedNode node;
    private final String vehicleId;
    private final File subTimelineDir;

    private final Map<String, Subscriber<ByteMultiArray>> active = new HashMap<>();

    public RegionSubscribeTimeline(ConnectedNode node, String vehicleId, String subTimelineDir) {
        this.node = node;
        this.vehicleId = vehicleId;
        this.subTimelineDir = new File(subTimelineDir);
    }

    @Override
    protected void setup() throws Exception {
        // ここに SubscriberTimelineLoop.setup() の処理を移す
    }

    @Override
    protected void loop() throws Exception {
        // ここに SubscriberTimelineLoop.loop() の処理を移す
        // 「どの regionId を subscribe するか決めて、Subscriber を作る/消す」など
    }

    private void ensureSubscriber(String regionId) {
        if (active.containsKey(regionId)) {
            return;
        }
        String safeRegionId = regionId.replaceAll("[^a-zA-Z0-9_]", "_");
        String topic = "v2x/region/" + safeRegionId + "/data";

        Subscriber<ByteMultiArray> sub =
                node.newSubscriber(topic, ByteMultiArray._TYPE);

        sub.addMessageListener(new MessageListener<ByteMultiArray>() {
            @Override
            public void onNewMessage(ByteMultiArray message) {
                try {
                    ChannelBuffer buf = message.getData();
                    byte[] payload = new byte[buf.readableBytes()];
                    buf.getBytes(buf.readerIndex(), payload);

                    // ここに SubscriberTimelineLoop の「受信した payload を保存する」処理を移す
                    // 例: VehicleTimelineNode.handlePayload(regionId, payload) みたいなメソッドを呼ぶ
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }
        });

        active.put(regionId, sub);
    }
}
*/