package v2x.vehicle.geo;

/**
 * 依存ライブラリなしの最小限GeoHashエンコーダ（WGS84）。 高速化やデコードは必要なら後で拡張。
 */
public final class GeoHash {

    private static final char[] BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();

    public static String encode(double lat, double lon, int precision) {
        double[] latRange = {-90.0, 90.0};
        double[] lonRange = {-180.0, 180.0};
        StringBuilder hash = new StringBuilder();
        boolean even = true;
        int bit = 0;
        int ch = 0;
        while (hash.length() < precision) {
            double mid;
            if (even) {
                mid = (lonRange[0] + lonRange[1]) / 2D;
                if (lon > mid) {
                    ch |= 1 << (4 - bit);
                    lonRange[0] = mid;
                } else {
                    lonRange[1] = mid;
                }
            } else {
                mid = (latRange[0] + latRange[1]) / 2D;
                if (lat > mid) {
                    ch |= 1 << (4 - bit);
                    latRange[0] = mid;
                } else {
                    latRange[1] = mid;
                }
            }
            even = !even;
            if (bit < 4) {
                bit++;
            } else {
                hash.append(BASE32[ch]);
                bit = 0;
                ch = 0;
            }
        }
        return hash.toString();
    }

    private GeoHash() {
    }
}
