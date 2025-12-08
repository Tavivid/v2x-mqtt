package v2x.vehicle.model;

import java.util.List;

/**
 * 単一リージョンの点群チャンク + 転送メタデータ。
 *
 * - regionId         : 領域ID (例: "cell-0a3c")
 * - sourceVehicleId  : 送信元 Vehicle ID (例: "vehicle-a")
 * - captureTsMillis  : 取得時刻 (sender 側で付ける)
 * - points           : 点群本体
 * - sourceFileName   : データセット元ファイル名 (例: "000123.pcd")
 */
public record PointCloudChunk(
        String regionId,
        String sourceVehicleId,
        long captureTsMillis,
        List<Point3D> points,
        String sourceFileName
) {
    /**
     * （必要なら）重複排除用キー。
     */
    public String makeDedupKey() {
        long bucket = captureTsMillis / 1000L;
        return regionId + "|" + sourceVehicleId + "|" + bucket;
    }
}
