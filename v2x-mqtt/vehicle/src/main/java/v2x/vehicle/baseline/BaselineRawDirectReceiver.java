package v2x.vehicle.baseline;

import v2x.vehicle.net.DirectTcpReceiver;

import java.io.File;
import java.nio.file.Files;

/**
 * Baseline receiver: saves PCD and prints latency.
 */
public final class BaselineRawDirectReceiver implements AutoCloseable {
    private final DirectTcpReceiver receiver;
    private final File recvDir;

    public BaselineRawDirectReceiver(int listenPort, String recvDir) {
        this.recvDir = new File(recvDir);
        this.receiver = new DirectTcpReceiver(listenPort, payload -> {
            try {
                long recvNano = System.nanoTime();
                BaselinePayloadCodec.Decoded d = BaselinePayloadCodec.decode(payload);

                this.recvDir.mkdirs();
                File out = new File(this.recvDir, d.fileName);
                Files.write(
                        out.toPath(),
                        java.util.Arrays.copyOfRange(payload, d.bodyOffset, d.bodyOffset + d.bodyLength)
                );

                long latencyNs = recvNano - d.sendNano;
                double latencyMs = latencyNs / 1_000_000.0;

                System.out.printf(
                        "[BASE-SUB] saved PCD: %s bytes=%d latency_ns=%d latency_ms=%.3f sendNano=%d recvNano=%d%n",
                        out.getAbsolutePath(),
                        d.bodyLength,
                        latencyNs,
                        latencyMs,
                        d.sendNano,
                        recvNano
                );
            } catch (Exception e) {
                System.err.println("[BASE-SUB] failed: " + e.getMessage());
            }
        });
    }

    public void start() throws Exception {
        receiver.start();
    }

    @Override
    public void close() throws Exception {
        receiver.close();
    }
}
