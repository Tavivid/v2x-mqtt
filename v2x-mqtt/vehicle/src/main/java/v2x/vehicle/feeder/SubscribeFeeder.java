package v2x.vehicle.feeder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.*;
import v2x.vehicle.request.RequestPublisher;

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
public class SubscribeFeeder {
  private final File csv;
  private final java.net.InetSocketAddress defaultRx;

  public SubscribeFeeder(File csv) {
    this.csv = csv;
    this.defaultRx = null;
  }

  public SubscribeFeeder(File csv, java.net.InetSocketAddress defaultRx) {
    this.csv = csv;
    this.defaultRx = defaultRx;
}

  static final class Row {
    final long atMs;
    final String region;
    final InetSocketAddress rx;
    final int priority;
    Row(long atMs, String region, InetSocketAddress rx, int priority) {
      this.atMs = atMs; this.region = region; this.rx = rx; this.priority = priority;
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
        if (tk.length < 4) throw new IllegalArgumentException("SUB CSV format error at line " + lineno + " (expect: at_ms,region_id,ip,port)");
        long at = Long.parseLong(tk[0].trim());
        String region = tk[1].trim();
        int priority = Integer.parseInt(tk[2].trim());
        String ipRaw = tk[3].trim();
        String portRaw = tk[4].trim();

        String ip = ipRaw;
        Integer port;

        if (ipRaw.isEmpty() || ipRaw.equalsIgnoreCase("auto") || ipRaw.equals("-")) {
          ip = (defaultRx != null) ? defaultRx.getAddress().getHostAddress() : "127.0.0.1";
        }
        if (portRaw.isEmpty() || portRaw.equalsIgnoreCase("auto") || portRaw.equals("-")) {
          if (defaultRx == null) throw new IllegalArgumentException("port empty and no defaultRx");
            port = defaultRx.getPort();
          } else {
            port = Integer.parseInt(portRaw);
          }
        rows.add(new Row(at, region, new InetSocketAddress(ip, port), priority));
      }
    }
    return rows.stream().sorted(Comparator.comparingLong(r -> r.atMs)).collect(Collectors.toList());
  }
  
  public void run(MqttClient client/*, DynamicSubscriptionManager dynSub*/) {
    try {
      List<Row> rows = load();
      if (rows.isEmpty()) { 
        System.out.println("[SUB] csv has no rows.");
        return;
      }
      long start = System.currentTimeMillis();
      ObjectMapper mapper = new ObjectMapper();

      RequestPublisher publisher = new RequestPublisher(client);

      for (Row r : rows) {
        long due = start + r.atMs;
        long now = System.currentTimeMillis();
        if (due > now) TimeUnit.MILLISECONDS.sleep(due - now);

        publisher.publish(r.region, r.rx, r.priority);
      }
      System.out.println("[SUB] csv sequence done");
    } catch (Exception e) {
      System.err.println("[SUB] error: " + e);
    }
  }
}
