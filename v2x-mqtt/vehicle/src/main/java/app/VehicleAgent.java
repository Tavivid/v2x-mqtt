package app;

import org.eclipse.paho.client.mqttv3.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.type.TypeReference;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class VehicleAgent {
  public static void main(String[] args) throws Exception {
    String broker = System.getenv().getOrDefault("BROKER_URI", "tcp://127.0.0.1:1883");
    String user   = System.getenv().getOrDefault("MQTT_USER", "");
    String pass   = System.getenv().getOrDefault("MQTT_PASS", "");
    String vehId  = System.getenv().getOrDefault("VEHICLE_ID", "vehicleA");
    String cid    = vehId + "-" + UUID.randomUUID();

    MqttClient client = new MqttClient(broker, cid);
    MqttConnectOptions opts = new MqttConnectOptions();
    opts.setAutomaticReconnect(true);
    opts.setCleanSession(true);
    if (!user.isBlank()) {
      opts.setUserName(user);
      opts.setPassword(pass.toCharArray());
    }
    client.connect(opts);

    ObjectMapper mapper = new ObjectMapper();

    client.subscribe("req/announce", (topic, msg) -> {
      String json = new String(msg.getPayload(), StandardCharsets.UTF_8);
      try {
        Map<String,Object> req = mapper.readValue(json, new TypeReference<Map<String,Object>>(){});
        String rid = String.valueOf(req.get("request_id"));
        String region = String.valueOf(req.get("region_id"));
        System.out.printf("[VEH %s] got RequestID=%s region=%s%n", vehId, rid, region);

        Map<String,Object> stats = Map.of(
          "vehicle_id", vehId,
          "request_id", rid,
          "received_ts_ms", System.currentTimeMillis()
        );
        byte[] payload = mapper.writeValueAsBytes(stats);
        MqttMessage m = new MqttMessage(payload);
        m.setQos(0);
        client.publish("stats/" + vehId, m);
      } catch (Exception e) {
        System.err.println("[VEH] invalid JSON: " + json);
      }
    });

    System.out.println("[VEH] subscribed; waiting...");
    Thread.currentThread().join();
  }
}
