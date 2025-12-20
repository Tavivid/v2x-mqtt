package v2x.vehicle.ros.timeline;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 共有メモリ(=mmap)で sendNano を渡すための簡易ストア。
 *
 * - region ごとに別ファイル（衝突を避けて実装を単純化） - seq (int) をキーとして sendNano (long) をリングに格納 -
 * 受信側は同じ region/seq のスロットから sendNano を読む
 *
 * 注意: - 同一ホストの複数プロセスで使う前提 - ファイルは /tmp に作る（環境変数で変更可）
 */
public final class SharedLatencyStore implements Closeable {

    // 環境変数
    private static final String DIR_ENV = "V2X_LAT_SHM_DIR";      // 例: /dev/shm
    private static final String RING_ENV = "V2X_LAT_RING_SIZE";   // 例: 131072 (2^n推奨)

    private static final int DEFAULT_RING_SIZE = 1 << 17; // 131072 slots
    private static final int SLOT_BYTES = 16; // [seq:int][pad:int][sendNano:long]

    private final Path baseDir;
    private final int ringSize;

    private final Map<String, RegionFile> regionFiles = new ConcurrentHashMap<>();

    public SharedLatencyStore() {
        String dir = System.getenv().getOrDefault(DIR_ENV, "/tmp");
        this.baseDir = Paths.get(dir);
        this.ringSize = parseIntEnv(RING_ENV, DEFAULT_RING_SIZE);
        try {
            Files.createDirectories(baseDir);
        } catch (IOException e) {
            throw new RuntimeException("cannot create shm dir: " + baseDir, e);
        }
    }

    private static int parseIntEnv(String key, int def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String sanitizeRegion(String regionId) {
        // ファイル名に使える程度に安全化
        // cell-083e -> cell-083e のまま、その他は '_' に
        return regionId.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private Path filePathForRegion(String regionId) {
        String safe = sanitizeRegion(regionId);
        // 例: /tmp/v2x_lat_cell-083e.bin
        return baseDir.resolve(String.format(Locale.ROOT, "v2x_lat_%s.bin", safe));
    }

    private RegionFile openRegion(String regionId) {
        return regionFiles.computeIfAbsent(regionId, rid -> {
            Path path = filePathForRegion(rid);
            long sizeBytes = (long) ringSize * SLOT_BYTES;

            try {
                FileChannel ch = FileChannel.open(
                        path,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                );
                // ファイルサイズを確保
                if (ch.size() < sizeBytes) {
                    ch.position(sizeBytes - 1);
                    ch.write(ByteBuffer.wrap(new byte[]{0}));
                    ch.force(true);
                }

                MappedByteBuffer mm = ch.map(FileChannel.MapMode.READ_WRITE, 0, sizeBytes);
                mm.order(ByteOrder.LITTLE_ENDIAN);

                return new RegionFile(ch, mm);
            } catch (IOException e) {
                throw new RuntimeException("cannot mmap: " + path, e);
            }
        });
    }

    private int slotIndex(int seq) {
        // ringSize は 2^n 推奨だが、そうでなくても動く
        int m = seq % ringSize;
        return (m < 0) ? (m + ringSize) : m;
    }

    private int slotOffsetBytes(int slotIdx) {
        return slotIdx * SLOT_BYTES;
    }

    /**
     * Publisher側：region, seq に対して sendNano を記録。
     */
    public void putSendNano(String regionId, int seq, long sendNano) {
        RegionFile rf = openRegion(regionId);
        int slot = slotIndex(seq);
        int off = slotOffsetBytes(slot);

        // スロット構造:
        // [0..3] seq(int)
        // [4..7] pad/int (未使用, 0)
        // [8..15] sendNano(long)
        // 書き順: sendNano -> seq （読み側が seq を確認して整合を取るため）
        rf.mm.putLong(off + 8, sendNano);
        rf.mm.putInt(off + 4, 0);
        rf.mm.putInt(off + 0, seq);
    }

    /**
     * Subscriber側：region, seq の sendNano を読む。 見つからない/上書き/未到達の場合は null。
     */
    public Long getSendNano(String regionId, int seq) {
        RegionFile rf = openRegion(regionId);
        int slot = slotIndex(seq);
        int off = slotOffsetBytes(slot);

        int storedSeq = rf.mm.getInt(off + 0);
        if (storedSeq != seq) {
            return null; // まだ書かれてない or 上書き済み
        }
        long sendNano = rf.mm.getLong(off + 8);
        if (sendNano == 0L) {
            // 0 は初期状態にもなり得るので、storedSeq一致が優先条件
            // ここは好みだが、0なら「未記録扱い」にしておく
            return null;
        }
        return sendNano;
    }

    @Override
    public void close() {
        // MappedByteBuffer は明示 close 不可。FileChannel だけ閉じる。
        for (RegionFile rf : regionFiles.values()) {
            try {
                rf.ch.close();
            } catch (IOException ignore) {
            }
        }
        regionFiles.clear();
    }

    private static final class RegionFile {

        final FileChannel ch;
        final MappedByteBuffer mm;

        RegionFile(FileChannel ch, MappedByteBuffer mm) {
            this.ch = ch;
            this.mm = mm;
        }
    }
}
