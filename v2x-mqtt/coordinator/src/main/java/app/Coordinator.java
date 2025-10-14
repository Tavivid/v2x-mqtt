package app;

import org.eclipse.paho.client.mqttv3.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.LinkedHashMap;

public class Coordinator {

  private static final Pattern REQ_TOPIC = Pattern.compile("^v2x/region/([^/]+)/request$");

  private static final ConcurrentHashMap<String, Long> regionDebounceUntil = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Long> seenHashes = new ConcurrentHashMap<>();

  public static void main(String[] args) throws Exception {
    String broker = env("BROKER_URI", "tcp://127.0.0.1:1883");
    String user   = env("MQTT_USER", "");
    String pass   = env("MQTT_PASS", "");
    long   ttlMs  = Long.parseLong(env("FETCH_TTL_MS", "1500"));
    int    qos    = Integer.parseInt(env("MQTT_QOS", "1")); // ★QoS=1推奨

    String cid = "v2x-coordinator-" + UUID.randomUUID();
    MqttClient client = new MqttClient(broker, cid);

    MqttConnectOptions opts = new MqttConnectOptions();
    opts.setAutomaticReconnect(true);
    opts.setCleanSession(false); // ★購読維持
    if (!user.isBlank()) { opts.setUserName(user); opts.setPassword(pass.toCharArray()); }

    client.setCallback(new MqttCallbackExtended() {
      @Override public void connectComplete(boolean reconnect, String serverURI) {
        System.out.println("[COORD] connectComplete reconnect=" + reconnect + " uri=" + serverURI);
        try {
          // 再接続時も確実に購読
          client.subscribe("v2x/region/+/request", qos);
          System.out.println("[COORD] (re)subscribed: v2x/region/+/request qos=" + qos);
        } catch (Exception e) {
          System.err.println("[COORD] re-subscribe failed: " + e);
        }
      }
      @Override public void connectionLost(Throwable cause) {
        System.err.println("[COORD] connectionLost: " + cause);
      }
      @Override public void messageArrived(String topic, MqttMessage message) { /* not used */ }
      @Override public void deliveryComplete(IMqttDeliveryToken token) { /* noop */ }
    });

    client.connect(opts);
    System.out.println("[COORD] connected cid=" + cid + " broker=" + broker);

    // 観察用
    client.subscribe("stats/#", (topic, msg) ->
        System.out.println("[STATS] " + topic + " -> " + new String(msg.getPayload(), StandardCharsets.UTF_8)));

    // 本質: /request 購読（QoS=1）
    client.subscribe("v2x/region/+/request", qos, (topic, msg) -> {
      String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);
      System.out.println("[COORD][RX] topic=" + topic + " retained=" + msg.isRetained() + " qos=" + msg.getQos()
          + " payload=" + payload);

      Matcher m = REQ_TOPIC.matcher(topic);
      if (!m.matches()) {
        System.out.println("[COORD][DROP] topic-not-match: " + topic);
        return;
      }
      String region = m.group(1);

      if (msg.isRetained()) {
        System.out.println("[COORD][DROP] retained");
        return;
      }

      String key = topic + "|" + sha1(payload);
      long now = System.currentTimeMillis();
      Long prev = seenHashes.put(key, now);
      if (prev != null && (now - prev) < 10_000) {
        System.out.println("[COORD][DROP] dedup within 10s key=" + key);
        return;
      }

      long until = regionDebounceUntil.getOrDefault(region, 0L);
      if (now < until) {
        System.out.println("[COORD][DROP] debounce region=" + region + " now=" + now + " until=" + until);
        return;
      }

      Map<String, Object> rxUdp = null;
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> reqObj = new ObjectMapper().readValue(payload, Map.class);
        Object rx = (reqObj != null) ? reqObj.get("rx_udp") : null;
        if (rx instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> rxMap = (Map<String, Object>) rx;
          Object ipObj = rxMap.get("ip");
          Object portObj = rxMap.get("port");
          if (ipObj != null && portObj instanceof Number) {
            rxUdp = Map.of("ip", String.valueOf(ipObj), "port", ((Number) portObj).intValue());
          }
        }
      } catch (Exception ignore) {
        // パース失敗時は rx_udp なしで続行（既存動作を壊さない）
      }
      if (rxUdp == null) {
        System.out.println("[COORD][INFO] rx_udp not found in request payload -> fetch-request will omit rx_udp");
      }

      String requestId = UUID.randomUUID().toString();
      Map<String, Object> fetch = new LinkedHashMap<>();
      fetch.put("type",       "fetch-request");
      fetch.put("region_id",  region);
      fetch.put("request_id", requestId);
      fetch.put("ts_ms",      now);
      fetch.put("ttl_ms",     ttlMs);
      if (rxUdp != null) {
        fetch.put("rx_udp", rxUdp);
      }

      try {
        byte[] out = new ObjectMapper().writeValueAsBytes(fetch);
        MqttMessage outMsg = new MqttMessage(out);
        outMsg.setQos(qos);            // ★QoS=1で出す
        outMsg.setRetained(false);
        String dataTopic = "v2x/region/" + region + "/data";
        System.out.println("[COORD][TX] -> " + dataTopic + " qos=" + qos + " payload=" + new String(out, StandardCharsets.UTF_8));
        client.publish(dataTopic, outMsg);
        System.out.println("[COORD] publish fetch-request -> topic=" + dataTopic + " " + fetch);
        regionDebounceUntil.put(region, now + ttlMs);
      } catch (Exception e) {
        System.err.println("[COORD][ERR] publish failed: " + e);
      }
    });

    System.out.println("[COORD] subscribed: v2x/region/+/request qos=" + qos);
    Thread.currentThread().join();
  }

  private static String env(String k, String def) {
    String v = System.getenv(k);
    return (v == null || v.isBlank()) ? def : v;
  }
  private static String sha1(String s) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte b : d) sb.append(String.format("%02x", b));
      return sb.toString();
    } catch (Exception e) {
      return "na";
    }
  }
}
