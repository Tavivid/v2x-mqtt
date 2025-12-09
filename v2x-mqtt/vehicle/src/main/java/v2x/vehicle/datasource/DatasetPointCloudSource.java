package v2x.vehicle.datasource;

import v2x.vehicle.model.Point3D;
import v2x.vehicle.model.PointCloudChunk;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * マルチリージョン対応のデータセットソース。
 *
 * ディレクトリ構成イメージ:
 *
 *   datasetRoot/
 *     cell-10/
 *       000000.pcd
 *       000001.pcd
 *       ...
 *     cell-12/
 *       000000.pcd
 *       000001.pcd
 *       ...
 *
 * - 通常モード（PointCloudSource インターフェース）:
 *     nextChunk(regionId, vehicleId, maxPoints) を呼ぶと、
 *     datasetRoot/<regionId>/ 以下のファイルを glob に従って列挙し、
 *     ソート順に 1 ファイルずつ消費してチャンクを返す。
 *
 * - フレーム番号指定モード（RegionTimelineFeeder 用の拡張）:
 *     nextChunkAtFrame(vehicleId, regionId, frameIndex, maxPoints) を呼ぶと、
 *     datasetRoot/<regionId>/<frameIndexを6桁ゼロ埋め>.pcd を直接読み込んで
 *     そのフレームの点群チャンクを返す。
 */
public class DatasetPointCloudSource implements PointCloudSource {

    private final Path datasetRoot;
    private final String globPattern;
    private final boolean loop;

    /**
     * 「通常モード」で region ごとにファイル列を順に読むための状態。
     */
    private static final class RegionCursor {
        final List<Path> files; // ソート済みファイル一覧
        int index = 0;

        RegionCursor(List<Path> files) {
            this.files = files;
        }
    }

    // regionId -> RegionCursor
    private final Map<String, RegionCursor> cursors = new ConcurrentHashMap<>();

    public DatasetPointCloudSource(String datasetRoot, String glob, boolean loop) throws IOException {
        this.datasetRoot = Paths.get(datasetRoot);
        if (!Files.isDirectory(this.datasetRoot)) {
            throw new IOException("Dataset root not found: " + datasetRoot);
        }
        this.globPattern = (glob == null || glob.isBlank()) ? "**/*" : glob;
        this.loop = loop;
    }

    // =========================================================
    // PointCloudSource インターフェース実装（従来モード）
    // =========================================================
    @Override
    public PointCloudChunk nextChunk(String regionId, String vehicleId, int maxPointsPerChunk) throws Exception {
        if (regionId == null || regionId.isBlank()) {
            return null;
        }

        RegionCursor cursor = cursors.computeIfAbsent(regionId, this::initCursorForRegion);
        if (cursor.files.isEmpty()) {
            // このリージョンにはファイルが 1 つも無い
            System.out.println("[DATASET] no files for region=" + regionId
                    + " dir=" + datasetRoot.resolve(regionId));
            return null;
        }

        // ファイルを 1 つずつ順番に消費
        if (cursor.index >= cursor.files.size()) {
            if (!loop) {
                // ループしない設定ならデータ枯渇
                return null;
            }
            cursor.index = 0;
        }

        Path file = cursor.files.get(cursor.index++);
        List<Point3D> pts = readPoints(file);
        if (pts.isEmpty()) {
            System.out.println("[DATASET] empty file region=" + regionId + " file=" + file);
            return null;
        }

        int n = Math.min(maxPointsPerChunk, pts.size());
        List<Point3D> sub = new ArrayList<>(pts.subList(0, n));
        long ts = Instant.now().toEpochMilli();

        String srcFileName = file.getFileName().toString();
        
        return new PointCloudChunk(regionId, vehicleId, ts, sub, srcFileName);
    }

    /**
     * regionId ごとのファイル列を初期化。
     * datasetRoot/<regionId>/ 以下のファイルを globPattern でフィルタし、ソートして保持する。
     */
    private RegionCursor initCursorForRegion(String regionId) {
        Path regionDir = datasetRoot.resolve(regionId);
        if (!Files.isDirectory(regionDir)) {
            System.out.println("[DATASET] region directory not found: " + regionDir);
            return new RegionCursor(List.of());
        }

        PathMatcher matcher = regionDir.getFileSystem().getPathMatcher("glob:" + globPattern);

        List<Path> files;
        try (Stream<Path> s = Files.walk(regionDir)) {
            files = s.filter(Files::isRegularFile)
                    .filter(p -> matcher.matches(regionDir.relativize(p)))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            System.err.println("[DATASET] failed to list files for region=" + regionId
                    + " dir=" + regionDir + " : " + e);
            return new RegionCursor(List.of());
        }

        if (files.isEmpty()) {
            System.out.println("[DATASET] no dataset files matched glob for region=" + regionId
                    + " dir=" + regionDir + " glob=" + globPattern);
            return new RegionCursor(List.of());
        }

        return new RegionCursor(files);
    }

    // =========================================================
    // フレーム番号指定版（RegionTimelineFeeder から呼ぶ拡張メソッド）
    // =========================================================

    /**
     * 指定した frameIndex の PCD を直接読む版。
     *
     * - datasetRoot/<regionId>/<frameIndexを6桁ゼロ埋め>.pcd
     *   例: frameIndex=3 → 000003.pcd
     *
     * @param vehicleId  ログ用（チャンクには埋め込んでいない）
     * @param regionId   対象リージョン
     * @param frameIndex タイムライン上のフレーム番号
     * @param maxPointsPerChunk 1 チャンクあたり最大点数
     */
    public PointCloudChunk nextChunkAtFrame(String vehicleId,
                                            String regionId,
                                            long frameIndex,
                                            int maxPointsPerChunk) throws Exception {
        if (regionId == null || regionId.isBlank()) {
            return null;
        }

        // 000000.pcd のようなファイル名を想定
        String fileName = String.format("%06d.pcd", frameIndex);
        Path regionDir = datasetRoot.resolve(regionId);
        Path file = regionDir.resolve(fileName);

        if (!Files.isRegularFile(file)) {
            // ファイルが無い場合は静かに null を返す（ログが多すぎる場合はコメントアウト可）
            System.out.println("[DATASET] frame file not found region=" + regionId
                    + " frame=" + frameIndex + " path=" + file);
            return null;
        }

        List<Point3D> pts = readPoints(file);
        if (pts.isEmpty()) {
            System.out.println("[DATASET] empty frame file region=" + regionId
                    + " frame=" + frameIndex + " path=" + file);
            return null;
        }

        int n = Math.min(maxPointsPerChunk, pts.size());
        List<Point3D> sub = new ArrayList<>(pts.subList(0, n));
        long ts = Instant.now().toEpochMilli();

        String srcFileName = file.getFileName().toString();

        return new PointCloudChunk(regionId, vehicleId, ts, sub, srcFileName);
    }

    // =========================================================
    // 共通：ファイルから Point3D リストを読む処理
    // =========================================================

    /**
     * 拡張子に応じてファイルから Point3D リストを読む。
     *
     * - *.pcd     -> PCD (DATA ascii / DATA binary) をパース
     * - その他    -> 1行を「x,y,z」または「x y z ...」として読むテキスト
     */
    private static List<Point3D> readPoints(Path p) throws IOException {
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".pcd")) {
            return readPcd(p);
        } else {
            return readCsvLike(p);
        }
    }

    /**
     * CSV/テキスト形式読み取り:
     * - 行頭 # や空行は無視
     * - カンマ or 空白で split
     * - 先頭3要素を x,y,z として float で読む
     */
    private static List<Point3D> readCsvLike(Path p) throws IOException {
        List<Point3D> pts = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;

                // カンマ or 空白区切り
                String[] a = line.split("[,\\s]+");
                if (a.length < 3) continue;

                try {
                    float x = Float.parseFloat(a[0].trim());
                    float y = Float.parseFloat(a[1].trim());
                    float z = Float.parseFloat(a[2].trim());
                    pts.add(new Point3D(x, y, z));
                } catch (NumberFormatException ignore) {
                    // 数値でなければスキップ
                }
            }
        }
        return pts;
    }

    /**
     * PCD ファイルを読む。
     *
     * - ヘッダ部分だけ ASCII として解析し、
     *   "DATA ascii" ならテキストとして、
     *   "DATA binary" ならバイナリとして x,y,z(float32) を読む。
     *
     * - DATA binary_compressed 等は未対応（空リスト + ログ）。
     */
    private static List<Point3D> readPcd(Path p) throws IOException {
        byte[] all = Files.readAllBytes(p);
        if (all.length == 0) return List.of();

        // ヘッダ部分だけ ASCII として解釈（最大 64KB まで）
        int headerLimit = Math.min(all.length, 65536);
        String headerStr = new String(all, 0, headerLimit, StandardCharsets.US_ASCII);

        int dataIdx = headerStr.indexOf("DATA");
        if (dataIdx < 0) {
            System.err.println("[DATASET] PCD header has no DATA line: " + p);
            return List.of();
        }
        int lineEndIdx = headerStr.indexOf('\n', dataIdx);
        if (lineEndIdx < 0) {
            lineEndIdx = headerLimit;
        }

        String dataLine = headerStr.substring(dataIdx, lineEndIdx).toUpperCase(Locale.ROOT);
        int dataOffset = lineEndIdx + 1; // "DATA ..." 行の次のバイトからデータ

        if (dataOffset >= all.length) {
            System.err.println("[DATASET] PCD has no data section: " + p);
            return List.of();
        }

        if (dataLine.contains("ASCII")) {
            // ===== DATA ascii =====
            int len = all.length - dataOffset;
            if (len <= 0) return List.of();

            String body = new String(all, dataOffset, len, StandardCharsets.US_ASCII);
            List<Point3D> pts = new ArrayList<>();
            for (String line : body.split("\\R")) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] a = line.split("\\s+");
                if (a.length < 3) continue;
                try {
                    float x = Float.parseFloat(a[0].trim());
                    float y = Float.parseFloat(a[1].trim());
                    float z = Float.parseFloat(a[2].trim());
                    pts.add(new Point3D(x, y, z));
                } catch (NumberFormatException ignore) {
                    // 数値でなければスキップ
                }
            }
            return pts;

        } else if (dataLine.contains("BINARY")) {
            // ===== DATA binary (非圧縮) =====
            // FIELDS x y z, SIZE 4 4 4, TYPE F F F を想定して
            // 12バイトずつ (float32 x, float32 y, float32 z) を読む。
            int payload = all.length - dataOffset;
            if (payload < 12) {
                System.err.println("[DATASET] PCD binary payload too short: " + p);
                return List.of();
            }

            int recordSize = 12; // x,y,z だけ扱う簡易版
            int num = payload / recordSize;

            ByteBuffer buf = ByteBuffer.wrap(all, dataOffset, num * recordSize)
                    .order(ByteOrder.LITTLE_ENDIAN);

            List<Point3D> pts = new ArrayList<>(num);
            for (int i = 0; i < num; i++) {
                float x = buf.getFloat();
                float y = buf.getFloat();
                float z = buf.getFloat();
                pts.add(new Point3D(x, y, z));
            }
            return pts;

        } else {
            // binary_compressed 等
            System.err.println("[DATASET] PCD DATA format unsupported (not ascii/binary): "
                    + dataLine + " file=" + p);
            return List.of();
        }
    }
}
