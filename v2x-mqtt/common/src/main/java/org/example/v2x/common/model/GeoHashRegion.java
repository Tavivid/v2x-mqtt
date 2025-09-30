package org.example.v2x.common.model;

public record GeoHashRegion(String geohash) {

    @Override
    public String toString() {
        return geohash;
    }
}
