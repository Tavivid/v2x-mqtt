package v2x.vehicle.datasource;

import v2x.vehicle.model.Point3D;
import v2x.vehicle.model.PointCloudChunk;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
 *       000.pcd
 *       001.pcd
 *       ...
 *     cell-12/
 *       000.pcd
 *       001.pcd
 *       ...
 *
 * - regionId ごとに datasetRoot/<regionId>/ を探し、
 * - その下のファイルを glob でフィルタしてソート、
 * - nextChunk(vehicleId, regionId, ...) が呼ばれるたびに
 *   対応リージョンのシーケンスから点群チャンクを返す。
 */
public class DatasetPointCloudSource implements PointCloudSource {

    private final Path datasetRoot;
    private final String globPattern;
    private final boolean loop;

    /**
     * リージョンごとの進捗状態
     */
    private static final class RegionState {
        final List<Path> files;        // このリージョンのファイル一覧（ソート済み）
        int fileIdx = 0;               // 今見ているファイルのインデックス
        List<Point3D> current = List.of(); // 現在のファイルから読んだ全ポイント
        int pointCursor = 0;           // current のどこまで消費したか

        RegionState(List<Path> files) {
            this.files = files;
        }
    }

    // regionId -> RegionState
    private final Map<String, RegionState> regions = new ConcurrentHashMap<>();

    public DatasetPointCloudSource(String datasetRoot, String glob, boolean loop) throws IOException {
        this.datasetRoot = Paths.get(datasetRoot);
        if (!Files.isDirectory(this.datasetRoot)) {
            throw new IOException("Dataset root not found: " + datasetRoot);
        }
        this.globPattern = (glob == null || glob.isBlank()) ? "**/*" : glob;
        this.loop = loop;
    }

    @Override
    public PointCloudChunk nextChunk(String vehicleId, String regionId, int maxPointsPerChunk) throws Exception {
        if (regionId == null || regionId.isBlank()) {
            return null;
        }

        RegionState st = regions.computeIfAbsent(regionId, this::initRegionState);
        if (st == null || st.files.isEmpty()) {
            // 指定リージョンに対応するファイルが1つも無い
            // （何度も出すとうるさいので最初だけにしたければフラグを持たせてもよい）
            System.out.println("[DATASET] no files for region=" + regionId
                    + " dir=" + datasetRoot.resolve(regionId));
            return null;
        }

        // current が空 or 消費し切っていたら次ファイルへ
        while (st.current.isEmpty() || st.pointCursor >= st.current.size()) {
            if (!advanceFile(st, regionId)) {
                // もうこれ以上ファイルがない（loop=false） -> データ枯渇
                return null;
            }
        }

        int remaining = st.current.size() - st.pointCursor;
        int n = Math.min(maxPointsPerChunk, remaining);
        List<Point3D> sub = new ArrayList<>(st.current.subList(st.pointCursor, st.pointCursor + n));
        st.pointCursor += n;

        long ts = Instant.now().toEpochMilli();
        return new PointCloudChunk(regionId, vehicleId, ts, sub);
    }

    /**
     * 指定リージョンの RegionState を初期化する。
     * datasetRoot/<regionId>/ 以下のファイルを globPattern でフィルタし、ソートして保持。
     */
    private RegionState initRegionState(String regionId) {
        Path regionDir = datasetRoot.resolve(regionId);
        if (!Files.isDirectory(regionDir)) {
            System.out.println("[DATASET] region directory not found: " + regionDir);
            return new RegionState(List.of());
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
            return new RegionState(List.of());
        }

        if (files.isEmpty()) {
            System.out.println("[DATASET] no dataset files matched glob for region=" + regionId
                    + " dir=" + regionDir + " glob=" + globPattern);
            return new RegionState(List.of());
        }

        RegionState st = new RegionState(files);
        try {
            st.current = readPoints(files.get(0));
            st.pointCursor = 0;
        } catch (IOException e) {
            System.err.println("[DATASET] failed to read first file for region=" + regionId
                    + " file=" + files.get(0) + " : " + e);
            st.current = List.of();
            st.pointCursor = 0;
        }
        return st;
    }

    /**
     * 指定リージョンで次のファイルへ進む（loop=true なら巻き戻し）。
     */
    private boolean advanceFile(RegionState st, String regionId) throws IOException {
        if (st.files.isEmpty()) return false;

        int next = st.fileIdx + 1;
        if (next >= st.files.size()) {
            if (!loop) {
                return false;
            }
            next = 0;
        }

        st.fileIdx = next;
        st.pointCursor = 0;
        Path file = st.files.get(next);
        st.current = readPoints(file);

        // 読んだ結果ポイントが 0 個なら、次ファイルに進んでみる
        // （全部 0 個だとループになるので、最大 files.size() 回まで試す）
        int guard = st.files.size();
        while (st.current.isEmpty() && guard-- > 0) {
            next = st.fileIdx + 1;
            if (next >= st.files.size()) {
                if (!loop) {
                    return false;
                }
                next = 0;
            }
            st.fileIdx = next;
            st.pointCursor = 0;
            file = st.files.get(next);
            st.current = readPoints(file);
        }

        return !st.current.isEmpty();
    }

    /**
     * 拡張子に応じてファイルから Point3D リストを読む。
     *
     * - *.pcd     -> ASCII PCD (DATA ascii) を簡易パース
     * - その他    -> 1行を「x,y,z」または「x y z ...」として読むテキスト
     */
    private static List<Point3D> readPoints(Path p) throws IOException {
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".pcd")) {
            return readPcd(p); // ★ 新しい PCD パーサ
        } else {
            return readCsvLike(p); // 従来どおり CSV/テキスト扱い
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
        String headerStr = new String(all, 0, headerLimit, java.nio.charset.StandardCharsets.US_ASCII);

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

            String body = new String(all, dataOffset, len, java.nio.charset.StandardCharsets.US_ASCII);
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
            // ※ 簡易実装: FIELDS x y z, SIZE 4 4 4, TYPE F F F を想定して
            //   12バイトずつ (float32 x, float32 y, float32 z) を読む。
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
