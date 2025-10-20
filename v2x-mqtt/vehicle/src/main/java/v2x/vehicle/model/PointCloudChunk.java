package v2x.vehicle.model;

import java.util.List;


/**
* シンプルな点群チャンク。実運用では圧縮/バイナリ転送を推奨。
*/
public record PointCloudChunk(String regionId, String sourceVehicleId, long captureTsMillis, List<Point3D> points) {

    public String makeDedupKey() {
// 領域ID + ソース車両 + 秒単位バケットでDPD（重複排除）を簡易に実現
        long bucket = captureTsMillis / 1000L;
        return regionId + "|" + sourceVehicleId + "|" + bucket;
    }
}