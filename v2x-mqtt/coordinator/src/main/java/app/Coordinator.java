package app;

import org.eclipse.paho.client.mqttv3.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class Coordinator {
  public static void main(String[] args) throws Exception {
    String broker = System.getenv().getOrDefault("BROKER_URI", "tcp://127.0.0.1:1883");
    String user   = System.getenv().getOrDefault("MQTT_USER", "");
    String pass   = System.getenv().getOrDefault("MQTT_PASS", "");
    String cid    = "coordinator-" + UUID.randomUUID();

    MqttClient client = new MqttClient(broker, cid);
    MqttConnectOptions opts = new MqttConnectOptions();
    opts.setAutomaticReconnect(true);
    opts.setCleanSession(true);
    if (!user.isBlank()) {
      opts.setUserName(user);
      opts.setPassword(pass.toCharArray());
    }
    client.connect(opts);

    // stats 受信（車両からの確認用）
    client.subscribe("stats/#", (topic, msg) -> {
      System.out.println("[STATS] " + topic + " -> " + new String(msg.getPayload(), StandardCharsets.UTF_8));
    });

    ObjectMapper mapper = new ObjectMapper();
    while (true) {
      Map<String,Object> req = Map.of(
        "request_id", UUID.randomUUID().toString(),
        "region_id",  "cell-12",
        "ttl_ms",     1500,
        "ts_ms",      System.currentTimeMillis()
      );
      byte[] payload = mapper.writeValueAsBytes(req);
      MqttMessage m = new MqttMessage(payload);
      m.setQos(0);
      m.setRetained(false);
      client.publish("req/announce", m);
      System.out.println("[COORD] published: " + req);
      Thread.sleep(5000);
    }
  }
}
