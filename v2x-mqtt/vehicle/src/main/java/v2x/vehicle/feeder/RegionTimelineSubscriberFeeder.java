package v2x.vehicle.feeder;

import v2x.vehicle.net.MasterClient;
import v2x.vehicle.tasks.SubscriberTask;
import v2x.vehicle.util.Jsons;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 「欲しい領域一覧」のタイムライン JSON に従って Subscriber を起動するフィーダ。
 *
 * - timelineDir: 例) ./sub-timeline
 *   - 000000.json
 *   - 000001.json
 *   - ...
 *
 * 各 JSON の形式:
 *   ["cell-0a3c", "cell-0a3d"]
 *   もしくは
 *   { "regions": ["cell-0a3c", "cell-0a3d"] }
 *
 * 特徴:
 * - publisher の有無に関わらず、stepMs 間隔で JSON を読み進める
 * - 各 frame ごとに「欲しい領域集合 currentRegions」を評価し、
 *   active な Subscriber と diff を取って start/stop する
 * - タイムライン側のスレッドは subscribe の成否でブロックしない
 */
public class RegionTimelineSubscriberFeeder implements Runnable {

    private final MasterClient master;
    private final File timelineDir;
    private final long stepMs;
    private final boolean loop;

    public RegionTimelineSubscriberFeeder(MasterClient master, File timelineDir, long stepMs, boolean loop) {
        this.master = master;
        this.timelineDir = timelineDir;
        this.stepMs = stepMs;
        this.loop = loop;
    }

    @Override
    public void run() {
        System.out.println("[SUB-TL] start dir=" + timelineDir.getAbsolutePath()
                + " stepMs=" + stepMs + " loop=" + loop);

        // タイムライン JSON 一覧を取得
        final List<Path> frames = listTimelineFiles(timelineDir.toPath());
        if (frames.isEmpty()) {
            System.err.println("[SUB-TL] no timeline json files under " + timelineDir.getAbsolutePath());
            return;
        }

        // region ごとの Subscriber 状態
        Map<String, SubscriberCtx> active = new HashMap<>();

        outerLoop:
        while (!Thread.currentThread().isInterrupted()) {
            for (int i = 0; i < frames.size(); i++) {
                Path framePath = frames.get(i);

                String frameName = framePath.getFileName().toString();
                System.out.println("[SUB-TL] start frameIndex=" + i + " file=" + frameName);

                List<String> regionList;
                try {
                    regionList = readRegionsFromJson(framePath);
                } catch (IOException e) {
                    System.err.println("[SUB-TL] failed to read timeline json: " + framePath + " : " + e);
                    regionList = List.of();
                }

                // このフレームで欲しい領域集合
                LinkedHashSet<String> currentRegions = new LinkedHashSet<>(regionList);

                // ---- 終了すべき Subscriber = active - current ----
                Set<String> toStop = new HashSet<>(active.keySet());
                toStop.removeAll(currentRegions);

                for (String regionId : toStop) {
                    SubscriberCtx ctx = active.remove(regionId);
                    if (ctx != null) {
                        System.out.println("[SUB-TL] stop subscriber for region=" + regionId
                                + " at frameIndex=" + i);
                        // スレッドに終了を依頼
                        ctx.thread.interrupt();
                    }
                }

                // ---- 新規に開始すべき Subscriber = current - active ----
                Set<String> toStart = new HashSet<>(currentRegions);
                toStart.removeAll(active.keySet());

                for (String regionId : toStart) {
                    SubscriberTask task = new SubscriberTask(master, regionId);
                    Thread t = new Thread(task, "Subscriber-" + regionId);
                    t.setDaemon(true);
                    t.start();
                    active.put(regionId, new SubscriberCtx(regionId, t));

                    System.out.println("[SUB-TL] started subscriber for region=" + regionId
                            + " at frameIndex=" + i);
                }

                // ---- フレーム間の待ち時間 ----
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

        // 終了時に Subscriber を全部止める
        for (SubscriberCtx ctx : active.values()) {
            ctx.thread.interrupt();
        }

        System.out.println("[SUB-TL] finished.");
    }

    private static class SubscriberCtx {
        final String regionId;
        final Thread thread;

        SubscriberCtx(String regionId, Thread thread) {
            this.regionId = regionId;
            this.thread = thread;
        }
    }

    // ====== ユーティリティ（RegionTimelineFeeder と同様） ======
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
