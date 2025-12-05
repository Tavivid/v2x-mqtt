package v2x.vehicle.feeder;

import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.datasource.PointCloudSource;
import v2x.vehicle.net.MasterClient;
import v2x.vehicle.tasks.PublisherTask;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * CSV を読み、指定タイミングで PublisherTask を起動するフィーダ（ROS1 版）。
 *
 * フォーマット: publish.csv
 *
 *   # at_ms,region_id,ip,port
 *   0,cell-10,auto,auto
 *   800,cell-12,auto,auto
 *   ...
 *
 * ただし ROS1 版では ip/port は使わず、先頭 2 列 (at_ms, region_id) のみを利用する。
 */
public class PublishFeeder {

    private final File csv;

    public PublishFeeder(File csv) {
        this.csv = csv;
    }

    static final class Row {
        final long atMs;
        final String region;

        Row(long atMs, String region) {
            this.atMs = atMs;
            this.region = region;
        }
    }

    private List<Row> load() throws IOException {
        List<Row> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csv))) {
            String line;
            long lineno = 0;
            while ((line = br.readLine()) != null) {
                lineno++;
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] tk = line.split(",", -1);
                if (tk.length < 2) {
                    throw new IllegalArgumentException(
                            "PUB CSV format error at line " + lineno + " (expect: at_ms,region_id[,...])");
                }
                long at = Long.parseLong(tk[0].trim());
                String region = tk[1].trim();
                rows.add(new Row(at, region));
            }
        }
        return rows.stream()
                .sorted(Comparator.comparingLong(r -> r.atMs))
                .collect(Collectors.toList());
    }

    /**
     * CSV に従って、指定タイミングで PublisherTask を起動する。
     *
     * @param cfg      AppConfig（dataset, vehicleId, maxPointsPerChunk など）
     * @param master   MasterClient（ROS1 風 Master 用）
     * @param pubHost  Publisher が advertise するホスト名（例: "127.0.0.1"）
     * @param basePort Publisher の TCP ポートのベース値（region ごとに +1 していく）
     */
    public void run(AppConfig cfg, MasterClient master, String pubHost, int basePort) {
        try {
            List<Row> rows = load();
            if (rows.isEmpty()) {
                System.out.println("[PUB] csv has no rows.");
                return;
            }

            long start = System.currentTimeMillis();
            Map<String, Integer> regionPorts = new HashMap<>();
            int nextPortOffset = 0;

            for (Row r : rows) {
                long due = start + r.atMs;
                long now = System.currentTimeMillis();
                if (due > now) {
                    TimeUnit.MILLISECONDS.sleep(due - now);
                }

                // 同じ region が複数行に出てきても、Publisher は 1 つだけ起動する
                if (regionPorts.containsKey(r.region)) {
                    System.out.println("[PUB] region=" + r.region + " already started; skip this row.");
                    continue;
                }

                int port = basePort + nextPortOffset++;

                PointCloudSource source;
                try {
                    source = new DatasetPointCloudSource(cfg.datasetPath, cfg.datasetGlob, cfg.datasetLoop);
                } catch (IOException e) {
                    System.err.println("[PUB] dataset not available for region=" + r.region + ": " + e.getMessage());
                    continue;
                }

                PublisherTask pubTask = new PublisherTask(
                        cfg,
                        source,
                        r.region,
                        master,
                        pubHost,
                        port
                );
                Thread th = new Thread(pubTask,
                        "Publisher-" + cfg.vehicleId + "-" + r.region);
                th.setDaemon(true);
                th.start();

                regionPorts.put(r.region, port);
                System.out.println("[PUB] started publisher region=" + r.region
                        + " host=" + pubHost + " port=" + port
                        + " (elapsed=" + (System.currentTimeMillis() - start) + "ms)");
            }

            System.out.println("[PUB] csv sequence done");
        } catch (Exception e) {
            System.err.println("[PUB] error: " + e);
            e.printStackTrace();
        }
    }
}
