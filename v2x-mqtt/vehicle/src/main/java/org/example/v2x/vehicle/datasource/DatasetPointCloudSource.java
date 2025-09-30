package org.example.v2x.vehicle.datasource;

import org.example.v2x.common.model.PointCloudChunk;

/**
 * 点群供給インターフェイス。データセット/センサ/ネットワークなど実装差し替え可能。
 */
public interface PointCloudSource {

    /**
     * 次のチャンクを返す（データが枯渇したら null）。
     */
    PointCloudChunk nextChunk(String regionId, String vehicleId, int maxPointsPerChunk) throws Exception;
}
```


---
# vehicle/src/main/java/org/example/v2x/vehicle/datasource/DatasetPointCloudSource.java
```java
package org.example.v2x.vehicle.datasource;


import org.example.v2x.common.model.Point3D;
import org.example.v2x.common.model.PointCloudChunk;


import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import java.util.stream.Collectors;


/**
* CSV (x,y,z) のファイル群から順にチャンクを生成する簡易実装。
* PLY/PCD は今後拡張（TODO）
*/
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
        try (var s = Files.walk(root)) {
            this.files = s.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".csv"))
                    .sorted()
                    .collect(Collectors.toList());
        }
        if (files.isEmpty()) {
            throw new IOException("No dataset files matched: " + glob);
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
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] a = line.split(",");
                if (a.length < 3) {
                    continue;
                }
                try {
                    float x = Float.parseFloat(a[0].trim());
                    float y = Float.parseFloat(a[1].trim());
                    float z = Float.parseFloat(a[2].trim());
                    pts.add(new Point3D(x, y, z));
                } catch (NumberFormatException ignore) {
                }
            }
        }
        return pts;
    }
}
