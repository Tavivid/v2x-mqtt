package v2x.vehicle.baseline;

import v2x.vehicle.net.DirectTcpSender;
import v2x.vehicle.ros.timeline.SharedLatencyStore;
import v2x.vehicle.ros.timeline.TimelineFiles;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.CRC32C;

public final class BaselineRawDirectSender implements AutoCloseable {

    private static final String LAT_REGION_ID = "baseline";
    private static final SharedLatencyStore LAT_STORE = new SharedLatencyStore();

    private final String datasetDir;
    private final DirectTcpSender sender;
    private final long stepMs;
    private final boolean loop;
    private final File timelineDir;

    public BaselineRawDirectSender(
            String datasetDir,
            String dstHost,
            int dstPort,
            long stepMs,
            boolean loop,
            File timelineDir
    ) {
        this.datasetDir = datasetDir;
        this.sender = new DirectTcpSender(dstHost, dstPort);
        this.stepMs = stepMs;
        this.loop = loop;
        this.timelineDir = timelineDir;
    }

    public void run() throws Exception {
        // jsonタイムラインがあるなら「フレーム数の上限」としてだけ使う（regionは無関係）
        int timelineSize = 0;
        if (timelineDir != null && timelineDir.isDirectory()) {
            try {
                List<Path> frames = TimelineFiles.listTimelineFiles(timelineDir.toPath());
                timelineSize = frames.size();
            } catch (Exception ignore) {}
        }

        int frameIndex = 0;
        while (true) {
            int idx = (timelineSize > 0) ? (frameIndex % timelineSize) : frameIndex;

            byte[] pcdBytes = readPcdBytes(idx);
            if (pcdBytes == null) {
                if (!loop) {
                    System.out.println("[BASE-PUB] finished. (no file) frame=" + idx);
                    break;
                }
                Thread.sleep(stepMs);
                frameIndex++;
                continue;
            }

            long sendNano = System.nanoTime();

            // 送るのは点群だけ
            sender.send(pcdBytes);

            // rosjava と同様：送信後に hash を計算し store に sendNano を記録
            int hash = crc32c(pcdBytes);
            LAT_STORE.putSendNanoByHash(LAT_REGION_ID, hash, sendNano);

            System.out.println(String.format(
                    "[BASE-PUB] sent RAW frame=%d file=%06d.pcd bytes=%d sendNano=%d dst=%s:%d",
                    idx, idx, pcdBytes.length, sendNano, getHost(), getPort()
            ));

            Thread.sleep(stepMs);
            frameIndex++;

            if (!loop && timelineSize > 0 && frameIndex >= timelineSize) {
                System.out.println("[BASE-PUB] finished. (timeline end) frames=" + timelineSize);
                break;
            }
        }
    }

    private byte[] readPcdBytes(int frameIndex) throws IOException {
        // baseline は datasetDir/%06d.pcd 前提（従来コメントと整合）
        Path p = Path.of(datasetDir).resolve(String.format("%06d.pcd", frameIndex));
        if (!Files.isRegularFile(p)) return null;
        return Files.readAllBytes(p);
    }

    private static int crc32c(byte[] data) {
        CRC32C c = new CRC32C();
        c.update(data, 0, data.length);
        return (int) c.getValue();
    }

    private String getHost() {
        // DirectTcpSender の host は private なのでログ用はここでは出せない。
        // BaselineRawDirectMain が "dst=host:port" を出しているため、ここは port だけで十分。
        return "?";
    }

    private int getPort() {
        // 同上：port を保持していないので表示は省略。
        return -1;
    }

    @Override
    public void close() {
        sender.close();
    }
}
