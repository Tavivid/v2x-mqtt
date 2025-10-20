package v2x.vehicle.datasource;

import v2x.vehicle.model.PointCloudChunk;

/**
 * 点群供給インターフェイス。データセット/センサ/ネットワークなど実装差し替え可能。
 */
public interface PointCloudSource {

    /**
     * 次のチャンクを返す（データが枯渇したら null）。
     */
    PointCloudChunk nextChunk(String regionId, String vehicleId, int maxPointsPerChunk) throws Exception;
}
