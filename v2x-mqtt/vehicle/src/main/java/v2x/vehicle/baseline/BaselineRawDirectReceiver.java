package v2x.vehicle.baseline;

import v2x.vehicle.net.DirectTcpReceiver;
import v2x.vehicle.ros.timeline.SharedLatencyStore;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

public final class BaselineRawDirectReceiver implements AutoCloseable {

    private static final String LAT_REGION_ID = "baseline";
    private static final SharedLatencyStore LAT_STORE = new SharedLatencyStore();

    // latency 計算（hash+lookup+log）を別スレッドへ
    private static final int LAT_QUEUE_CAP = 8192;
    private static final ThreadPoolExecutor LAT_EXEC;
    static {
        LAT_EXEC = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(LAT_QUEUE_CAP),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "baseline-latency-worker");
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
        LAT_EXEC.prestartAllCoreThreads();
    }

    private final File recvDir;
    private final AtomicInteger seq = new AtomicInteger(0);
    private final DirectTcpReceiver receiver;

    public BaselineRawDirectReceiver(int listenPort, String recvDir) {
        this.recvDir = new File(recvDir);
        this.recvDir.mkdirs();

        this.receiver = new DirectTcpReceiver(listenPort, obj -> {
            byte[] payload = (byte[]) obj;
            if (payload == null || payload.length == 0) return;

            try {
                int id = seq.getAndIncrement();
                File out = new File(this.recvDir, String.format("%06d.pcd", id));

                // 保存（メイン受信処理）
                Files.write(out.toPath(), payload);
                final long saveNano = System.nanoTime();
                final int bytes = payload.length;

                // latency 計算は別スレッド
                final byte[] raw = payload;
                Runnable task = () -> {
                    try {
                        int hash = crc32c(raw);
                        Long sendNano = LAT_STORE.getSendNanoByHash(LAT_REGION_ID, hash);

                        if (sendNano != null) {
                            long latencyNs = saveNano - sendNano;
                            double latencyMs = latencyNs / 1_000_000.0;
                            System.out.println(String.format(
                                    "[BASE-SUB] saved PCD: %s bytes=%d latency_ns=%d latency_ms=%.3f sendNano=%d saveNano=%d",
                                    out.getAbsolutePath(), bytes, latencyNs, latencyMs, sendNano, saveNano
                            ));
                        } else {
                            System.out.println(String.format(
                                    "[BASE-SUB] saved PCD: %s bytes=%d saveNano=%d (sendNano not found)",
                                    out.getAbsolutePath(), bytes, saveNano
                            ));
                        }
                    } catch (Exception e) {
                        System.err.println("[BASE-SUB] latency-worker failed: " + e.getMessage());
                    }
                };

                try {
                    LAT_EXEC.execute(task);
                } catch (RejectedExecutionException rej) {
                    System.out.println(String.format(
                            "[BASE-SUB] saved PCD: %s bytes=%d saveNano=%d (latency skipped: queue full)",
                            out.getAbsolutePath(), bytes, saveNano
                    ));
                }

            } catch (Exception e) {
                System.err.println("[BASE-SUB] failed: " + e.getMessage());
            }
        });
    }

    public void start() {
        receiver.start();
    }

    private static int crc32c(byte[] data) {
        CRC32C c = new CRC32C();
        c.update(data, 0, data.length);
        return (int) c.getValue();
    }

    @Override
    public void close() {
        receiver.close();
    }
}
