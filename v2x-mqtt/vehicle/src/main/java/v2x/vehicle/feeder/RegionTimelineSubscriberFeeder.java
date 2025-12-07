package v2x.vehicle.feeder;

import v2x.vehicle.net.MasterClient;
import v2x.vehicle.tasks.SubscriberTask;
import v2x.vehicle.util.Jsons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 「欲しい領域タイムライン JSON」を元に Subscriber を動的に起動/停止するフィーダ。
 *
 * - SUB_TIMELINE_DIR ... JSON が並んでいるディレクトリ（VehicleMain 側で指定）
 * - SUB_TIMELINE_STEP_MS ... フレーム間隔（デフォルト 10000ms = 0.1Hz）
 * - SUB_TIMELINE_LOOP ... "1" なら最後まで行ったら先頭に戻る
 *
 * JSON フォーマット（1ファイル = 1タイムステップ）:
 *
 *   1) 配列形式（推奨）
 *      ["cell-0a3c", "cell-0a3d"]
 *
 *   2) オブジェクト形式
 *      { "regions": ["cell-0a3c", "cell-0a3d"] }
 */
public class RegionTimelineSubscriberFeeder implements Runnable {

    private final MasterClient master;
    private final File timelineDir;
    private final long stepMs;
    private final boolean loop;

    public RegionTimelineSubscriberFeeder(MasterClient master,
                                          File timelineDir,
                                          long stepMs,
                                          boolean loop) {
        this.master = master;
        this.timelineDir = timelineDir;
        this.stepMs = stepMs;
        this.loop = loop;
    }

    @Override
    public void run() {
        try {
            List<File> frames = listTimelineFiles(timelineDir);
            if (frames.isEmpty()) {
                System.out.println("[SUB-TL] no timeline json in dir=" + timelineDir.getAbsolutePath());
                return;
            }

            // regionId -> Subscriber スレッド
            Map<String, Thread> subscribers = new HashMap<>();

            do {
                for (File f : frames) {
                    long frameStart = System.currentTimeMillis();

                    // このフレームで「欲しい領域」一覧
                    Set<String> desired = new LinkedHashSet<>(readRegionsFromJson(f));

                    // 1) もう欲しくない領域は unsubscribe（= スレッド interrupt）
                    Iterator<Map.Entry<String, Thread>> it = subscribers.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, Thread> e = it.next();
                        String regionId = e.getKey();
                        if (!desired.contains(regionId)) {
                            System.out.println("[SUB-TL] stop subscriber region=" + regionId
                                    + " (no longer requested in " + f.getName() + ")");
                            e.getValue().interrupt(); // SubscriberTask 側で sleep が InterruptedException になって終了
                            it.remove();
                        }
                    }

                    // 2) 新たに欲しくなった領域は subscribe 開始
                    for (String regionId : desired) {
                        if (subscribers.containsKey(regionId)) {
                            continue; // 既に購読中
                        }
                        SubscriberTask subTask = new SubscriberTask(master, regionId);
                        Thread th = new Thread(subTask, "Subscriber-" + regionId);
                        th.setDaemon(true);
                        th.start();
                        subscribers.put(regionId, th);
                        System.out.println("[SUB-TL] started subscriber region=" + regionId
                                + " from frame=" + f.getName());
                    }

                    long elapsed = System.currentTimeMillis() - frameStart;
                    long sleep = stepMs - elapsed;
                    if (sleep > 0) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(sleep);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }

                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                }
            } while (loop && !Thread.currentThread().isInterrupted());

            // 終了時に全部止める
            for (Thread th : subscribers.values()) {
                th.interrupt();
            }
            subscribers.clear();
            System.out.println("[SUB-TL] stopped all subscribers.");

        } catch (Exception e) {
            System.err.println("[SUB-TL] error: " + e);
            e.printStackTrace();
        }
    }

    /**
     * timeline ディレクトリ内の *.json を名前順にソートして返す
     */
    private static List<File> listTimelineFiles(File dir) {
        File[] arr = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"));
        if (arr == null) return Collections.emptyList();
        Arrays.sort(arr, Comparator.comparing(File::getName));
        return Arrays.asList(arr);
    }

    /**
     * 1フレーム分 JSON から regionId リストを読む
     */
    @SuppressWarnings("unchecked")
    private static List<String> readRegionsFromJson(File file) throws java.io.IOException {
        try (Reader r = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {

            Object obj = Jsons.GSON.fromJson(r, Object.class);
            if (obj == null) {
                return Collections.emptyList();
            }

            if (obj instanceof List) {
                // ["cell-0a3c", "cell-0a3d"]
                List<?> raw = (List<?>) obj;
                List<String> regions = new ArrayList<>();
                for (Object o : raw) {
                    if (o != null) regions.add(o.toString());
                }
                return regions;
            } else if (obj instanceof Map) {
                // { "regions": [...] }
                Map<?, ?> map = (Map<?, ?>) obj;
                Object v = map.get("regions");
                if (v instanceof List) {
                    List<?> raw = (List<?>) v;
                    List<String> regions = new ArrayList<>();
                    for (Object o : raw) {
                        if (o != null) regions.add(o.toString());
                    }
                    return regions;
                }
            }

            throw new IllegalArgumentException(
                    "timeline json must be array or {\"regions\":[...]}, file=" + file.getName());
        }
    }
}
