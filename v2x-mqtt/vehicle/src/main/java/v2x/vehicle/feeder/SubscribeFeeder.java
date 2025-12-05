package v2x.vehicle.feeder;

import v2x.vehicle.net.MasterClient;
import v2x.vehicle.tasks.SubscriberTask;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * CSV を読み、指定タイミングで SubscriberTask を起動するフィーダ（ROS1 版）。
 *
 * フォーマット: subscribe.csv
 *
 *   # at_ms,region_id,priority,ip,port
 *   0,cell-10,3,auto,auto
 *   5000,cell-12,3,auto,auto
 *
 * - at_ms    : フィーダ開始からの相対ミリ秒
 * - region_id: 購読したいリージョン
 * - priority : 現状はログに出すだけ（将来的に優先度制御などに利用可）
 * - ip/port  : 以前は UDP 受信先だったが、ROS1 版ではログ用途のみ（auto/空は defaultRx に置き換え）
 */
public class SubscribeFeeder {

    private final File csv;
    private final InetSocketAddress defaultRx;

    public SubscribeFeeder(File csv) {
        this(csv, null);
    }

    public SubscribeFeeder(File csv, InetSocketAddress defaultRx) {
        this.csv = csv;
        this.defaultRx = defaultRx;
    }

    static final class Row {
        final long atMs;
        final String region;
        final InetSocketAddress rx;
        final int priority;

        Row(long atMs, String region, InetSocketAddress rx, int priority) {
            this.atMs = atMs;
            this.region = region;
            this.rx = rx;
            this.priority = priority;
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
                if (tk.length < 5) {
                    throw new IllegalArgumentException(
                            "SUB CSV format error at line " + lineno + " (expect: at_ms,region_id,priority,ip,port)");
                }
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
                    if (defaultRx == null) {
                        throw new IllegalArgumentException("port empty and no defaultRx");
                    }
                    port = defaultRx.getPort();
                } else {
                    port = Integer.parseInt(portRaw);
                }

                rows.add(new Row(at, region, new InetSocketAddress(ip, port), priority));
            }
        }
        return rows.stream()
                .sorted(Comparator.comparingLong(r -> r.atMs))
                .collect(Collectors.toList());
    }

    /**
     * CSV に従って、指定タイミングで SubscriberTask を起動する。
     *
     * NOTE:
     *   - 同じ region に対しては 1 つだけ Subscriber を起動する。
     *   - priority/ip/port は今はログ用途のみ（将来的に UDP 連携などに利用可）。
     */
    public void run(MasterClient master) {
        try {
            List<Row> rows = load();
            if (rows.isEmpty()) {
                System.out.println("[SUB] csv has no rows.");
                return;
            }

            long start = System.currentTimeMillis();
            Map<String, Thread> subscribers = new HashMap<>();

            for (Row r : rows) {
                long due = start + r.atMs;
                long now = System.currentTimeMillis();
                if (due > now) {
                    TimeUnit.MILLISECONDS.sleep(due - now);
                }

                if (subscribers.containsKey(r.region)) {
                    System.out.println("[SUB] region=" + r.region + " already subscribed; skip this row.");
                    continue;
                }

                SubscriberTask subTask = new SubscriberTask(master, r.region);
                Thread th = new Thread(subTask,
                        "Subscriber-" + r.region + "-" + r.rx.getAddress().getHostAddress() + ":" + r.rx.getPort());
                th.setDaemon(true);
                th.start();
                subscribers.put(r.region, th);

                System.out.println("[SUB] started subscriber region=" + r.region
                        + " rx=" + r.rx
                        + " priority=" + r.priority
                        + " (elapsed=" + (System.currentTimeMillis() - start) + "ms)");
            }

            System.out.println("[SUB] csv sequence done");
        } catch (Exception e) {
            System.err.println("[SUB] error: " + e);
            e.printStackTrace();
        }
    }
}
