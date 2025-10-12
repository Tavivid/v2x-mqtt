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

/**
 * Coordinator（/request を受けた時だけ fetch-request を流す）
 * - これまで「/request 以外のトピックに来た場合」は何も表示されず分かりづらかったため、
 *   監視専用で /data にも subscribe し「間違いだと思われる投稿」を明示ログに出すように改善。
 */
public class Coordinator {

  // v2x/region/{region}/request を厳密に扱う
  private static final Pattern REQ_TOPIC = Pattern.compile("^v2x/region/([^/]+)/request$");
  // 監視用（誤って /data に流れたものを検知）
  private static final Pattern DATA_TOPIC = Pattern.compile("^v2x/region/([^/]+)/data$");

  // 連発防止（TTL内は同一regionの再発行を抑制）
  private static final ConcurrentHashMap<String, Long> regionDebounceUntil = new ConcurrentHashMap<>();
  // 同一メッセージの重複処理を避ける（QoS再送・再接続時の再配信対策）
  private static final ConcurrentHashMap<String, Long> seenHashes = new ConcurrentHashMap<>();

  public static void main(String[] args) throws Exception {
    String broker = env("BROKER_URI", "tcp://127.0.0.1:1883");
    String user   = env("MQTT_USER", "");
    String pass   = env("MQTT_PASS", "");
    long   ttlMs  = Long.parseLong(env("FETCH_TTL_MS", "1500"));  // fetch-request の有効期限
    int    qos    = Integer.parseInt(env("MQTT_QOS", "0"));

    String cid = "v2x-coordinator-" + UUID.randomUUID();
    MqttClient client = new MqttClient(broker, cid);

    MqttConnectOptions opts = new MqttConnectOptions();
    opts.setAutomaticReconnect(true);
    opts.setCleanSession(true);
    if (!user.isBlank()) { opts.setUserName(user); opts.setPassword(pass.toCharArray()); }

    client.connect(opts);
    System.out.println("[COORD] connected cid=" + cid + " broker=" + broker);

    // 任意: 車両からの stats 可視化
    client.subscribe("stats/#", (topic, msg) ->
        System.out.println("[STATS] " + topic + " -> " + new String(msg.getPayload(), StandardCharsets.UTF_8)));

    // ★ 監視追加：誤って /data に投稿された “要求っぽい” メッセージを検知して注意喚起
    client.subscribe("v2x/region/+/data", (topic, msg) -> {
      try {
        String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);
        Matcher m = DATA_TOPIC.matcher(topic);
        if (!m.matches()) return;
        String region = m.group(1);
        System.out.println("[COORD][NOTICE] received on /data (ignored): topic=" + topic +
            " retained=" + msg.isRetained() + " payload=" + payload);
        System.out.println("                 Did you mean to publish to v2x/region/" + region + "/request ?");
      } catch (Exception e) {
        System.err.println("[COORD][NOTICE] /data monitor error: " + e);
      }
    });

    // 本質：/request を“受けたときだけ”処理
    client.subscribe("v2x/region/+/request", (topic, msg) -> {
      try {
        String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);
        System.out.println("[COORD] recv topic=" + topic + " retained=" + msg.isRetained() + " payload=" + payload);

        Matcher m = REQ_TOPIC.matcher(topic);
        if (!m.matches()) {
          System.out.println("[COORD] topic not match (ignored): " + topic);
          return;  // 厳密マッチ
        }
        String region = m.group(1);

        // retained は無視（過去の残りに反応しない）
        if (msg.isRetained()) {
          System.out.println("[COORD] ignore retained request topic=" + topic);
          return;
        }

        // 重複抑止: topic + payload のハッシュで10秒キャッシュ
        String key = topic + "|" + sha1(payload);
        long now = System.currentTimeMillis();
        Long prev = seenHashes.put(key, now);
        if (prev != null && (now - prev) < 10_000) {
          System.out.println("[COORD] dedup same request within 10s topic=" + topic);
          return;
        }

        // TTL 内デバウンス: 同一 region は新しい fetch-request を抑制
        long until = regionDebounceUntil.getOrDefault(region, 0L);
        if (now < until) {
          System.out.println("[COORD] debounce active region=" + region + " until=" + until);
          return;
        }

        // request_id を発行して fetch-request を /data に流す（現行の運用に合わせる）
        String requestId = UUID.randomUUID().toString();
        Map<String, Object> fetch = Map.of(
            "type",       "fetch-request",
            "region_id",  region,
            "request_id", requestId,
            "ts_ms",      now,
            "ttl_ms",     ttlMs
        );

        byte[] out = new ObjectMapper().writeValueAsBytes(fetch);
        MqttMessage outMsg = new MqttMessage(out);
        outMsg.setQos(qos);
        outMsg.setRetained(false);

        String dataTopic = "v2x/region/" + region + "/data";
        client.publish(dataTopic, outMsg);

        // デバウンス更新
        regionDebounceUntil.put(region, now + ttlMs);

        System.out.println("[COORD] publish fetch-request -> topic=" + dataTopic + " " + fetch);
      } catch (Exception e) {
        System.err.println("[COORD] handler error: " + e);
        e.printStackTrace();
      }
    });

    System.out.println("[COORD] subscribed: v2x/region/+/request (main), v2x/region/+/data (notice)");
    // メインスレッドを生かしておく
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
