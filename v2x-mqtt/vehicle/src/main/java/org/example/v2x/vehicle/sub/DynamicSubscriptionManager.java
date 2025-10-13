package org.example.v2x.vehicle.sub;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * CSVで投げるリージョンに合わせて、/data or /control の購読を動的に増減する。
 * - touch(region) を呼ぶと ensure subscribe
 * - 一定時間使われないリージョンは自動で unsubscribe
 */
public class DynamicSubscriptionManager implements AutoCloseable {

  private final MqttClient client;
  private final String topicSuffix; // "data" or "control"
  private final long idleMs;
  private final IMqttMessageListener listener;

  private final ConcurrentMap<String, Long> lastTouched = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Boolean> subscribed = new ConcurrentHashMap<>();
  private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "dyn-sub-pruner");
    t.setDaemon(true); return t;
  });

  public DynamicSubscriptionManager(MqttClient client, String topicSuffix, long idleMs, IMqttMessageListener listener) {
    this.client = client;
    this.topicSuffix = Objects.requireNonNull(topicSuffix);
    this.idleMs = idleMs;
    this.listener = Objects.requireNonNull(listener);
  }

  /** 周期的にアイドル購読を解除するタスクを開始 */
  public void start() {
    ses.scheduleAtFixedRate(this::pruneIdle, idleMs, Math.max(1000L, idleMs / 3), TimeUnit.MILLISECONDS);
  }

  /** そのリージョンを直近使用としてマーク、未購読なら subscribe する */
  public void touch(String region) {
    long now = System.currentTimeMillis();
    lastTouched.put(region, now);
    if (!subscribed.containsKey(region)) {
      String topic = topicFor(region);
      try {
        client.subscribe(topic, /*qos*/0, listener);
        subscribed.put(region, Boolean.TRUE);
        System.out.println("[DYN] subscribed " + topic);
      } catch (Exception e) {
        System.err.println("[DYN] subscribe failed topic=" + topic + " err=" + e);
      }
    }
  }

  /** 明示的に解除したいとき */
  public void unsubscribe(String region) {
    String topic = topicFor(region);
    try {
      client.unsubscribe(topic);
      subscribed.remove(region);
      lastTouched.remove(region);
      System.out.println("[DYN] unsubscribed " + topic);
    } catch (Exception e) {
      System.err.println("[DYN] unsubscribe failed topic=" + topic + " err=" + e);
    }
  }

  private String topicFor(String region) {
    return "v2x/region/" + region + "/" + topicSuffix;
  }

  /** 一定時間触られていない購読を解除 */
  private void pruneIdle() {
    long now = System.currentTimeMillis();
    for (Map.Entry<String, Long> e : lastTouched.entrySet()) {
      String region = e.getKey();
      long last = e.getValue();
      if (now - last >= idleMs) {
        unsubscribe(region);
      }
    }
  }

  @Override public void close() {
    ses.shutdownNow();
    for (String r : subscribed.keySet()) {
      try { client.unsubscribe(topicFor(r)); } catch (Exception ignore) {}
    }
    subscribed.clear();
    lastTouched.clear();
  }
}
