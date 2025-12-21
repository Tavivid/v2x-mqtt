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
 * 共有メモリ(mmap)で sendNano を渡すストア（hashキー版）
 *
 * - regionごとに別ファイル
 * - key = CRC32C(rawPcdBytes)（int）
 * - slot = key % ringSize
 * - スロットには [hash(int)][pad(int)][sendNano(long)] を保存
 *
 * 注意:
 * - CRC32C は衝突可能（ただし実運用では十分低い想定）
 * - 衝突/上書きが起きた場合は get が null になるか、別データの時刻を拾う可能性がある
 *   → まずは軽量性優先で length は入れない（要望通り）
 */
public final class SharedLatencyStore implements Closeable {

    private static final String DIR_ENV = "V2X_LAT_SHM_DIR";     // 例: /dev/shm
    private static final String RING_ENV = "V2X_LAT_RING_SIZE";  // 例: 131072

    private static final String DEFAULT_DIR = "/dev/shm";
    private static final int DEFAULT_RING_SIZE = 1 << 17; // 131072
    private static final int SLOT_BYTES = 16; // [hash:int][pad:int][sendNano:long]

    private final Path baseDir;
    private final int ringSize;

    private final Map<String, RegionFile> regionFiles = new ConcurrentHashMap<>();

    public SharedLatencyStore() {
        // ★ 未設定なら /dev/shm を使う。ダメなら /tmp にフォールバック。
        String dirEnv = System.getenv(DIR_ENV);
        String dir = (dirEnv == null || dirEnv.isBlank()) ? DEFAULT_DIR : dirEnv.trim();

        Path dirPath = Paths.get(dir);
        try {
            Files.createDirectories(dirPath);
        } catch (IOException e) {
            // /dev/shm が無い/権限が無い等の保険
            dirPath = Paths.get("/tmp");
            try {
                Files.createDirectories(dirPath);
            } catch (IOException e2) {
                throw new RuntimeException("cannot create shm dir: " + dirPath, e2);
            }
        }
        this.baseDir = dirPath;

        // ★ 未設定なら 131072（既存の DEFAULT_RING_SIZE をそのまま使う）
        this.ringSize = parseIntEnv(RING_ENV, DEFAULT_RING_SIZE);
    }

    private static int parseIntEnv(String key, int def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String sanitizeRegion(String regionId) {
        return regionId.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private Path filePathForRegion(String regionId) {
        String safe = sanitizeRegion(regionId);
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

    private int slotIndexByHash(int hash) {
        // 負数対策
        int h = hash & 0x7fffffff;
        int m = h % ringSize;
        return (m < 0) ? (m + ringSize) : m;
    }

    private static int slotOffsetBytes(int slotIdx) {
        return slotIdx * SLOT_BYTES;
    }

    /** Publisher側：region + hash に対して sendNano を記録 */
    public void putSendNanoByHash(String regionId, int hash, long sendNano) {
        RegionFile rf = openRegion(regionId);
        int slot = slotIndexByHash(hash);
        int off = slotOffsetBytes(slot);

        // 書き順：sendNano -> hash（読み側が hash を見て整合チェック）
        rf.mm.putLong(off + 8, sendNano);
        rf.mm.putInt(off + 4, 0);
        rf.mm.putInt(off + 0, hash);
    }

    /** Subscriber側：region + hash の sendNano を読む。見つからない/上書きなら null */
    public Long getSendNanoByHash(String regionId, int hash) {
        RegionFile rf = openRegion(regionId);
        int slot = slotIndexByHash(hash);
        int off = slotOffsetBytes(slot);

        int storedHash = rf.mm.getInt(off + 0);
        if (storedHash != hash) {
            return null;
        }
        long sendNano = rf.mm.getLong(off + 8);
        if (sendNano == 0L) {
            return null;
        }
        return sendNano;
    }

    @Override
    public void close() {
        for (RegionFile rf : regionFiles.values()) {
            try {
                rf.ch.close();
            } catch (IOException ignore) {}
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
