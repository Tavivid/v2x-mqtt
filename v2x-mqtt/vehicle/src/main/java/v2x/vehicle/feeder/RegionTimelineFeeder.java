package v2x.vehicle.feeder;

import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.datasource.PointCloudSource;
import v2x.vehicle.model.PointCloudChunk;
import v2x.vehicle.net.MasterClient;
import v2x.vehicle.net.Topics;
import v2x.vehicle.net.RosPublisher;
import v2x.vehicle.util.PointCloudSerializer;
import v2x.vehicle.util.Jsons;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 時系列 JSON に従って「どのフレームでどの領域を持っているか」を決め、
 * フレームごとに 10Hz(=stepMs)で送信する Publisher 群のオーケストレータ。
 *
 * - timelineDir: 例) ./timeline
 *   - 000000.json
 *   - 000001.json
 *   - ...
 *
 * 各 JSON の形式:
 *   ["cell-0a3c", "cell-0a3d"]
 *   もしくは
 *   { "regions": ["cell-0a3c", "cell-0a3d"] }
 *
 * Dataset は AppConfig の datasetPath/datasetGlob/datasetLoop を使う。
 * 各 region について、出現するたびに DatasetPointCloudSource.nextChunk() を 1回呼び、
 * そのチャンクを ROS publisher から送信する。
 */
public class RegionTimelineFeeder implements Runnable {

    private final AppConfig cfg;
    private final MasterClient master;
    private final String vehicleId;
    private final String pubHost;
    private final int basePubPort;
    private final File timelineDir;
    private final long stepMs;
    private final boolean loop;

    public RegionTimelineFeeder(AppConfig cfg,
                                MasterClient master,
                                String vehicleId,
                                String pubHost,
                                int basePubPort,
                                File timelineDir,
                                long stepMs,
                                boolean loop) {
        this.cfg = cfg;
        this.master = master;
        this.vehicleId = vehicleId;
        this.pubHost = pubHost;
        this.basePubPort = basePubPort;
        this.timelineDir = timelineDir;
        this.stepMs = stepMs;
        this.loop = loop;
    }

    @Override
    public void run() {
        System.out.println("[Timeline] start dir=" + timelineDir.getAbsolutePath()
                + " stepMs=" + stepMs + " loop=" + loop);

        // Dataset 読み込み
        final PointCloudSource source;
        try {
            source = new DatasetPointCloudSource(cfg.datasetPath, cfg.datasetGlob, cfg.datasetLoop);
        } catch (Exception e) {
            System.err.println("[Timeline] dataset not available: " + e.getMessage());
            return;
        }

        // タイムライン JSON 一覧を取得
        final List<Path> frames = listTimelineFiles(timelineDir.toPath());
        if (frames.isEmpty()) {
            System.err.println("[Timeline] no timeline json files under " + timelineDir.getAbsolutePath());
            return;
        }

        // region ごとの Publisher 状態
        Map<String, RegionPublisherCtx> active = new HashMap<>();
        int portOffset = 0;

        outerLoop:
        while (!Thread.currentThread().isInterrupted()) {
            for (int i = 0; i < frames.size(); i++) {
                Path framePath = frames.get(i);
                long frameIndex = i; // ファイル名から取っても良いが、とりあえず添字

                String frameName = framePath.getFileName().toString();
                System.out.println("[Timeline] start frameIndex=" + i + " file=" + frameName);

                List<String> regionList;
                try {
                    regionList = readRegionsFromJson(framePath);
                } catch (IOException e) {
                    System.err.println("[Timeline] failed to read timeline json: " + framePath + " : " + e);
                    regionList = List.of();
                }

                // このフレームでアクティブな領域集合
                LinkedHashSet<String> currentRegions = new LinkedHashSet<>(regionList);

                // ---- Publisher の開始・終了制御 ----
                // 終了すべき領域 = active - current
                Set<String> toStop = new HashSet<>(active.keySet());
                toStop.removeAll(currentRegions);

                for (String regionId : toStop) {
                    RegionPublisherCtx ctx = active.remove(regionId);
                    if (ctx != null) {
                        System.out.println("[Timeline] stop publisher for region=" + regionId);
                        try {
                            ctx.publisher.close();
                        } catch (Exception ignore) {
                        }
                    }
                }

                // 新規に開始すべき領域 = current - active
                Set<String> toStart = new HashSet<>(currentRegions);
                toStart.removeAll(active.keySet());

                for (String regionId : toStart) {
                    int port = basePubPort + (++portOffset);
                    String topic = Topics.data(regionId);
                    RosPublisher rp = new RosPublisher(master, topic, pubHost, port);
                    long t0 = System.currentTimeMillis();
                    try {
                        rp.start();
                        long elapsed = System.currentTimeMillis() - t0;
                        System.out.println("[Timeline] started publisher region=" + regionId
                                + " host=" + pubHost + " port=" + port + " (elapsed=" + elapsed + "ms)");
                        active.put(regionId, new RegionPublisherCtx(regionId, rp));
                    } catch (Exception e) {
                        System.err.println("[Timeline] failed to start publisher for region=" + regionId + " : " + e);
                        try {
                            rp.close();
                        } catch (Exception ignore) {
                        }
                    }
                }

                // ---- このフレームでアクティブな領域に 1回ずつ送信 ----
                for (String regionId : currentRegions) {
                    RegionPublisherCtx ctx = active.get(regionId);
                    if (ctx == null) {
                        // Publisher 起動に失敗したなど
                        continue;
                    }

                    if (!ctx.publisher.hasSubscribers()) {
                        // 必要ならデバッグ用に 1 回だけログ出すフラグを持たせても OK
                        // System.out.println("[PUB] no subscribers for region=" + regionId + " -> skip");
                        continue;
                    }

                    try {
                        PointCloudChunk chunk = ((DatasetPointCloudSource) source)
                                .nextChunk(vehicleId, regionId, cfg.maxPointsPerChunk);
                        if (chunk == null) {
                            // その領域のデータが尽きている
                            System.out.println("[DATASET] no more data for region=" + regionId);
                            continue;
                        }

                        int pointCount = (chunk.points() == null) ? 0 : chunk.points().size();

                        if (pointCount <= 10) {
                            /*
                            System.out.println("[PUB] skip frame=" + frameIndex
                                    + " region=" + regionId
                                    + " vehicleId=" + vehicleId
                                    + " points=" + pointCount + " (<=10)");
                            */
                            continue;
                        }

                        byte[] payload = PointCloudSerializer.serialize(vehicleId, regionId, chunk);
                        ctx.publisher.publish(payload);

                        //int pointCount = (chunk.points() == null) ? 0 : chunk.points().size();
                        //System.out.println("[PUB] frame=" + frameIndex
                        //        + " region=" + regionId
                        //        + " vehicleId=" + vehicleId
                        //        + " points=" + pointCount);
                    } catch (Exception e) {
                        System.err.println("[PUB] error while sending frame=" + frameIndex
                                + " region=" + regionId + " : " + e);
                    }
                }

                // ---- フレーム間の待ち時間（10Hz 等）----
                try {
                    Thread.sleep(stepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break outerLoop;
                }
            }

            if (!loop) {
                break;
            }
        }

        // 終了時に Publisher を全部閉じる
        for (RegionPublisherCtx ctx : active.values()) {
            try {
                ctx.publisher.close();
            } catch (Exception ignore) {
            }
        }

        System.out.println("[Timeline] finished.");
    }

    private static class RegionPublisherCtx {
        final String regionId;
        final RosPublisher publisher;

        RegionPublisherCtx(String regionId, RosPublisher publisher) {
            this.regionId = regionId;
            this.publisher = publisher;
        }
    }

    private static List<Path> listTimelineFiles(Path dir) {
        File[] files = dir.toFile().listFiles((d, name) -> name.endsWith(".json"));
        if (files == null) {
            return List.of();
        }
        return Arrays.stream(files)
                .map(File::toPath)
                .sorted(Comparator.comparing(Path::getFileName))
                .collect(Collectors.toList());
    }

    /**
     * JSON から領域一覧を読む。
     *  - ["cell-0a3c", "cell-0a3d"]
     *  - {"regions":["cell-0a3c","cell-0a3d"]}
     * の両方を許容。
     */
    private static List<String> readRegionsFromJson(Path jsonPath) throws IOException {
        String text = Files.readString(jsonPath, StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            return List.of();
        }

        // 配列だけの形式 ["cell-0a3c", ...]
        if (text.startsWith("[")) {
            String[] arr = Jsons.GSON.fromJson(text, String[].class);
            return Arrays.asList(arr);
        }

        // オブジェクト形式 {"regions":[...]} を想定
        JsonObject obj = Jsons.GSON.fromJson(text, JsonObject.class);
        JsonElement regionsElem = obj.get("regions");
        if (regionsElem == null || !regionsElem.isJsonArray()) {
            return List.of();
        }
        JsonArray arr = regionsElem.getAsJsonArray();
        List<String> result = new ArrayList<>(arr.size());
        for (JsonElement e : arr) {
            if (e.isJsonPrimitive()) {
                result.add(e.getAsString());
            }
        }
        return result;
    }
}
