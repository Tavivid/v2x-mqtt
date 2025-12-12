package v2x.vehicle.ros.timeline;

public final class TimelineTopics {

    private TimelineTopics() {}

    public static String topicForRegion(String regionId) {
        String safeRegionId = regionId.replaceAll("[^a-zA-Z0-9_]", "_");
        return "/v2x/region/" + safeRegionId + "/data";
    }
}
