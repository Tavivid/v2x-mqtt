package v2x.vehicle.ros.timeline;

public final class TimelineLog {

    private TimelineLog() {}

    // INFO系: stdout
    public static void log(String tag, String msg) {
        System.out.printf("[%s] %s%n", tag, msg);
    }

    public static void logf(String tag, String fmt, Object... args) {
        log(tag, String.format(fmt, args));
    }

    // ERROR系: stderr（stderrは使ってOK）
    public static void error(String tag, String msg) {
        System.err.printf("[%s] %s%n", tag, msg);
    }

    public static void error(String tag, String msg, Throwable t) {
        error(tag, msg);
        if (t != null) t.printStackTrace(System.err);
    }
}
