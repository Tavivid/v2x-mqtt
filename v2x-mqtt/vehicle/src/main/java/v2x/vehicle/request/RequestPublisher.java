package v2x.vehicle.request;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class RequestPublisher {
  private final MqttClient client;
  private final ObjectMapper mapper = new ObjectMapper();

  public RequestPublisher(MqttClient client) {
    this.client = client;
  }

  public void publish(String regionId, InetSocketAddress rx, int priority) {
    try {
      Map<String, Object> body = Map.of(
          "type", "need-pointcloud",
          "region_id", regionId,
          "priority", priority,
          "rx_udp", Map.of("ip", rx.getAddress().getHostAddress(), "port", rx.getPort()),
          "ts_ms", System.currentTimeMillis(),
          "nonce", UUID.randomUUID().toString()
      );
      byte[] payload = mapper.writeValueAsBytes(body);
      MqttMessage msg = new MqttMessage(payload);
      msg.setQos(1);          // ★ 0 → 1 に変更（取りこぼし低減）
      msg.setRetained(false);

      String topic = "v2x/region/" + regionId + "/request";
      client.publish(topic, msg);
      System.out.printf("[SUB] sended request topic=%s payload=%s%n", topic, new String(payload, StandardCharsets.UTF_8));
    } catch (Exception e) {
      System.err.println("[SUB] request send failed: " + e);
    }
  }
}
