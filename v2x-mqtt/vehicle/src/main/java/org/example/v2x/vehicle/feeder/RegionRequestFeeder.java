package org.example.v2x.vehicle.feeder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import org.example.v2x.vehicle.request.RequestPublisher;

import java.io.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CSV を読み、指定タイミングで /request を投げる簡易フィーダ。
 *
 * フォーマット:
 *   # at_ms,region_id,ip,port
 *   0,cell-12,127.0.0.1,51235
 *   1000,cell-13,127.0.0.1,51235
 *
 * - at_ms: フィーダ開始からの相対ミリ秒
 * - region_id: 要求リージョン
 * - ip/port: Requester(=このVehicle)のUDP受信先
 */
public class RegionRequestFeeder {
  private final File csv;

  public RegionRequestFeeder(File csv) {
    this.csv = csv;
  }

  static final class Row {
    final long atMs;
    final String region;
    final InetSocketAddress rx;
    Row(long atMs, String region, InetSocketAddress rx) {
      this.atMs = atMs; this.region = region; this.rx = rx;
    }
  }

  private List<Row> load() throws Exception {
    List<Row> rows = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(csv))) {
      String line;
      long lineno = 0;
      while ((line = br.readLine()) != null) {
        lineno++;
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) continue;
        String[] tk = line.split(",", -1);
        if (tk.length < 4) throw new IllegalArgumentException("REQ CSV format error at line " + lineno + " (expect: at_ms,region_id,ip,port)");
        long at = Long.parseLong(tk[0].trim());
        String region = tk[1].trim();
        String ip = tk[2].trim();
        int port = Integer.parseInt(tk[3].trim());
        rows.add(new Row(at, region, new InetSocketAddress(ip, port)));
      }
    }
    return rows.stream().sorted(Comparator.comparingLong(r -> r.atMs)).collect(Collectors.toList());
  }
  
  public void run(MqttClient client/*, DynamicSubscriptionManager dynSub*/) {
    try {
      List<Row> rows = load();
      if (rows.isEmpty()) { 
        System.out.println("[FEED-REQ] no rows.");
        return;
      }
      long start = System.currentTimeMillis();
      ObjectMapper mapper = new ObjectMapper();

      for (Row r : rows) {
        long due = start + r.atMs;
        long now = System.currentTimeMillis();
        if (due > now) TimeUnit.MILLISECONDS.sleep(due - now);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", "need-pointcloud");
        body.put("region_id", r.region);
        body.put("rx_udp", Map.of("ip", r.rx.getAddress().getHostAddress(), "port", r.rx.getPort()));
        body.put("ts_ms", System.currentTimeMillis());
        body.put("nonce", UUID.randomUUID().toString());

        byte[] payload = mapper.writeValueAsBytes(body);
        MqttMessage msg = new MqttMessage(payload);
        msg.setQos(1);          // 取りこぼし低減のため QoS=1
        msg.setRetained(false);

        String topic = "v2x/region/" + r.region + "/request";
        client.publish(topic, msg);
        System.out.printf("[FEED-REQ] published topic=%s payload=%s%n",
            topic, new String(payload, StandardCharsets.UTF_8));
      }
      System.out.println("[FEED-REQ] sequence done");
    } catch (Exception e) {
      System.err.println("[FEED-REQ] error: " + e);
    }
  }
}
