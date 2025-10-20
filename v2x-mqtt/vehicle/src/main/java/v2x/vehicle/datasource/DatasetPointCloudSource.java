package v2x.vehicle.datasource;

import v2x.vehicle.model.Point3D;
import v2x.vehicle.model.PointCloudChunk;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class DatasetPointCloudSource implements PointCloudSource {

    private final List<Path> files;
    private final boolean loop;
    private int fileIdx = 0;
    private int pointCursor = 0;
    private List<Point3D> current = List.of();

    public DatasetPointCloudSource(String datasetRoot, String glob, boolean loop) throws IOException {
        Path root = Paths.get(datasetRoot);
        if (!Files.exists(root)) {
            throw new IOException("Dataset path not found: " + datasetRoot);
        }
        final PathMatcher matcher = (glob != null && !glob.isBlank())
                ? root.getFileSystem().getPathMatcher("glob:" + glob)
                : root.getFileSystem().getPathMatcher("glob:**/*.csv");

        try (var s = Files.walk(root)) {
            this.files = s.filter(Files::isRegularFile)
                    .filter(p -> matcher.matches(root.relativize(p)))
                    .sorted()
                    .collect(Collectors.toList());
        }
        if (files.isEmpty()) {
            throw new IOException("No dataset files matched glob: " + (glob == null ? "(null)" : glob));
        }
        this.loop = loop;
        loadFile(0);
    }

    private void loadFile(int idx) throws IOException {
        this.fileIdx = idx;
        this.pointCursor = 0;
        this.current = readCsv(files.get(idx));
    }

    private static List<Point3D> readCsv(Path p) throws IOException {
        List<Point3D> pts = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] a = line.split(",");
                if (a.length < 3) continue;
                try {
                    float x = Float.parseFloat(a[0].trim());
                    float y = Float.parseFloat(a[1].trim());
                    float z = Float.parseFloat(a[2].trim());
                    pts.add(new Point3D(x, y, z));
                } catch (NumberFormatException ignore) { /* skip bad line */ }
            }
        }
        return pts;
    }

    /** PointCloudSource の仕様：見つからない/終端の場合は null を返す実装にしています。 */
    @Override
    public PointCloudChunk nextChunk(String vehicleId, String region, int maxPointsPerChunk) {
        try {
            if (current.isEmpty()) {
                if (!advanceFile()) return null;
            }

            int remaining = current.size() - pointCursor;
            if (remaining <= 0) {
                if (!advanceFile()) return null;
                remaining = current.size() - pointCursor;
                if (remaining <= 0) return null;
            }

            int n = Math.min(maxPointsPerChunk, remaining);
            List<Point3D> sub = current.subList(pointCursor, pointCursor + n);
            List<Point3D> copy = new ArrayList<>(sub); // subList の独立コピー
            pointCursor += n;

            long ts = Instant.now().toEpochMilli();
            // ★ あなたの PointCloudChunk のコンストラクタに合わせて調整してください
            // 例: new PointCloudChunk(vehicleId, region, ts, copy)
            return new PointCloudChunk(vehicleId, region, ts, copy);

        } catch (IOException e) {
            // 読み替えや次ファイル読み込みに失敗した場合は null（上位でリトライ想定）
            return null;
        }
    }

    /** 次ファイルへ（loop=true なら巻き戻し） */
    private boolean advanceFile() throws IOException {
        int next = fileIdx + 1;
        if (next >= files.size()) {
            if (!loop) return false;
            next = 0;
        }
        loadFile(next);
        return true;
    }
}
