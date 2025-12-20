package v2x.vehicle.ros.timeline;

public enum PcdPayloadMode {
    /**
     * [4:nameLen][nameBytes][pcdBytes] (現状互換)
     */
    NAMED,
    /**
     * [8:sendTimeMs][4:nameLen][nameBytes][pcdBytes]
     */
    NAMED_WITH_SEND_TIME_MS,
    /**
     * [pcdBytes only]
     */
    RAW_ONLY;

    public static PcdPayloadMode fromEnv() {
        String v = System.getenv().getOrDefault("PCD_PAYLOAD_MODE", "NAMED").trim();
        if (v.isEmpty()) {
            return NAMED;
        }
        switch (v.toUpperCase()) {
            case "RAW":
            case "RAW_ONLY":
            case "BODY_ONLY":
                return RAW_ONLY;
            case "NAMED_TS":
            case "NAMED_WITH_TS":
            case "NAMED_WITH_SEND_TIME_MS":
                return NAMED_WITH_SEND_TIME_MS;
            case "NAMED":
            default:
                return NAMED;
        }
    }
}
