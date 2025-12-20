package v2x.vehicle.ros;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.ros.address.InetAddressFactory;
import org.ros.node.DefaultNodeMainExecutor;
import org.ros.node.NodeConfiguration;
import org.ros.node.NodeMainExecutor;

import java.net.URI;

/**
 * VehicleTimelineNode を起動するだけのエントリポイント。
 *
 * 使う環境変数: ROS_MASTER_URI : 例) http://127.0.0.1:11311 ROS_IP / ROS_HOSTNAME:
 * このノード自身のIP (省略時は自動推定を試みる)
 */
public class VehicleRosMain {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.ros.internal").setLevel(Level.WARNING);
        Logger.getLogger("org.ros.internal.node.client.Registrar").setLevel(Level.WARNING);
        Logger.getLogger("org.ros.internal.node.topic.DefaultPublisher").setLevel(Level.WARNING);

        // ROS Master URI を環境変数から取る
        String masterUriStr = System.getenv("ROS_MASTER_URI");
        if (masterUriStr == null || masterUriStr.isBlank()) {
            System.err.println("ROS_MASTER_URI is not set.");
            System.exit(1);
        }
        URI masterUri = new URI(masterUriStr);

        // 自ノードの IP
        String host = System.getenv("ROS_IP");
        if (host == null || host.isBlank()) {
            host = InetAddressFactory.newNonLoopback().getHostAddress();
        }

        System.out.println("[VehicleRosMain] ROS_MASTER_URI=" + masterUri);
        System.out.println("[VehicleRosMain] host=" + host);

        NodeConfiguration nodeConfig = NodeConfiguration.newPublic(host);
        nodeConfig.setMasterUri(masterUri);

        VehicleTimelineNode node = new VehicleTimelineNode();

        NodeMainExecutor executor = DefaultNodeMainExecutor.newDefault();
        executor.execute(node, nodeConfig);

        // executor がノードライフサイクルを持つので main はここで終わらない
    }
}
