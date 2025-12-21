package v2x.vehicle.baseline;

import v2x.vehicle.datasource.FlatDatasetPointCloudSource;
import v2x.vehicle.net.DirectTcpSender;
import v2x.vehicle.ros.timeline.TimelineFiles;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Baseline sender loop.
 */
public final class BaselineRawDirectSender implements AutoCloseable {
    private final FlatDatasetPointCloudSource src;
    private final DirectTcpSender sender;
    private final long stepMs;
    private final boolean loop;
    private final int timelineSizeHint;

    public BaselineRawDirectSender(
            String datasetDir,
            String dstHost,
            int dstPort,
            long stepMs,
            boolean loop,
            File timelineDir
    ) throws Exception {
        this.src = new FlatDatasetPointCloudSource(datasetDir);
        this.sender = new DirectTcpSender(dstHost, dstPort);
        this.stepMs = stepMs;
        this.loop = loop;
        this.timelineSizeHint = loadTimelineSizeHint(timelineDir);
    }

    private static int loadTimelineSizeHint(File timelineDir) {
        try {
            if (timelineDir != null && timelineDir.isDirectory()) {
                List<Path> timelineFrames = TimelineFiles.listTimelineFiles(timelineDir.toPath());
                return timelineFrames.size();
            }
        } catch (Exception ignored) {
        }
        return 0;
    }

    /**
     * Blocking run. Returns when finished (non-loop) or never (loop).
     */
    public void run() throws Exception {
        int frameIndex = 0;

        while (true) {
            int idx = (timelineSizeHint > 0) ? (frameIndex % timelineSizeHint) : frameIndex;

            FlatDatasetPointCloudSource.RawPcd raw = src.readRawPcdWithName(idx);
            if (raw == null) {
                if (!loop) {
                    System.out.println("[BASE-PUB] finished. (no file) frame=" + idx);
                    return;
                }
                // loop時: 少し待ってリトライ（datasetが遅れて生成されるケースにも耐える）
                TimeUnit.MILLISECONDS.sleep(stepMs);
                frameIndex++;
                continue;
            }

            long sendNano = System.nanoTime();
            byte[] payload = BaselinePayloadCodec.encode(sendNano, raw.fileName, raw.data);
            sender.send(payload);

            System.out.println("[BASE-PUB] sent RAW frame=" + idx
                    + " file=" + raw.fileName
                    + " bytes=" + payload.length
                    + " sendNano=" + sendNano);

            TimeUnit.MILLISECONDS.sleep(stepMs);
            frameIndex++;

            if (!loop && timelineSizeHint > 0 && frameIndex >= timelineSizeHint) {
                System.out.println("[BASE-PUB] finished. (timeline end) frames=" + timelineSizeHint);
                return;
            }
        }
    }

    @Override
    public void close() throws Exception {
        sender.close();
    }
}
