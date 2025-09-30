package org.example.v2x.common.model;

public record VehicleId(String id) {

    @Override
    public String toString() {
        return id;
    }
}
