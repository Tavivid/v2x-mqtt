package v2x.vehicle.datasource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 領域分割しない（フラット）なデータセット用:
 *   datasetRoot/
 *     000000.pcd
 *     000001.pcd
 *     ...
 */
public final class FlatDatasetPointCloudSource {

  private final Path datasetRoot;

  public static final class RawPcd {
    public final String fileName;
    public final byte[] data;
    public RawPcd(String fileName, byte[] data) {
      this.fileName = fileName;
      this.data = data;
    }
  }

  public FlatDatasetPointCloudSource(String datasetRoot) throws IOException {
    this.datasetRoot = Paths.get(datasetRoot);
    if (!Files.isDirectory(this.datasetRoot)) {
      throw new IOException("Dataset root not found: " + datasetRoot);
    }
  }

  public RawPcd readRawPcdWithName(int frameIndex) throws IOException {
    String fileName = String.format("%06d.pcd", frameIndex);
    Path file = datasetRoot.resolve(fileName);
    if (!Files.isRegularFile(file)) {
      return null;
    }
    byte[] bytes = Files.readAllBytes(file);
    return new RawPcd(file.getFileName().toString(), bytes);
  }
}
