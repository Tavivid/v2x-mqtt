package v2x.vehicle.ros;

import org.ros.namespace.GraphName;
import org.ros.node.AbstractNodeMain;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Publisher;
import std_msgs.ByteMultiArray;
import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.ros.timeline.PublisherTimelineLoop;
import v2x.vehicle.ros.timeline.SubscriberTimelineLoop;
import v2x.vehicle.ros.timeline.TimelineConfig;
import v2x.vehicle.ros.timeline.TimelineFiles;
import v2x.vehicle.ros.timeline.TimelineLog;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VehicleTimelineNode extends AbstractNodeMain {

    private final AppConfig cfg;
    private final String vehicleId;

    private final TimelineConfig.Pub pubCfg;
    private final TimelineConfig.Sub subCfg;

    // region -> Publisher
    private final Map<String, Publisher<ByteMultiArray>> publisherPool = new HashMap<>();

    public VehicleTimelineNode() {
        this.cfg = AppConfig.load();
        this.vehicleId = cfg.vehicleId;

        this.pubCfg = TimelineConfig.Pub.fromEnv();
        this.subCfg = TimelineConfig.Sub.fromEnv();
    }

    @Override
    public GraphName getDefaultNodeName() {
        String safeVehicleId = vehicleId.replaceAll("[^a-zA-Z0-9_]", "_");
        return GraphName.of("v2x_vehicle_timeline/" + safeVehicleId);
    }

    @Override
    public void onStart(final ConnectedNode connectedNode) {

        // ============================
        // Publisher timeline
        // ============================
        if (pubCfg.timelineDir != null && pubCfg.timelineDir.isDirectory()) {
            final List<Path> pubFrames = TimelineFiles.listTimelineFiles(pubCfg.timelineDir.toPath());
            if (pubFrames.isEmpty()) {
                connectedNode.getLog().error("[PUB-TL] no timeline json under " + pubCfg.timelineDir.getAbsolutePath());
            } else {
                final DatasetPointCloudSource datasetSource;
                try {
                    String datasetRoot = pubCfg.timelineDir.getAbsolutePath();
                    datasetSource = new DatasetPointCloudSource(datasetRoot, cfg.datasetGlob, cfg.datasetLoop);
                } catch (Exception e) {
                    connectedNode.getLog().error("[PUB-TL] dataset not available: " + e.getMessage(), e);
                    return;
                }

                connectedNode.executeCancellableLoop(
                        new PublisherTimelineLoop(
                                connectedNode,
                                datasetSource,
                                pubFrames,
                                pubCfg.stepMs,
                                pubCfg.loop,
                                vehicleId,
                                cfg.maxPointsPerChunk,
                                publisherPool,
                                pubCfg.syncUnixSec
                        )
                );

                TimelineLog.logf("PUB-TL",
                        "started timeline dir=%s stepMs=%d loop=%s syncUnix=%d",
                        pubCfg.timelineDir.getAbsolutePath(), pubCfg.stepMs, pubCfg.loop, pubCfg.syncUnixSec);
            }
        }

        // ============================
        // Subscriber timeline
        // ============================
        if (subCfg.timelineDir != null && subCfg.timelineDir.isDirectory()) {
            final List<Path> subFrames = TimelineFiles.listTimelineFiles(subCfg.timelineDir.toPath());
            if (!subFrames.isEmpty()) {
                connectedNode.executeCancellableLoop(
                        new SubscriberTimelineLoop(
                                connectedNode,
                                subFrames,
                                subCfg.stepMs,
                                subCfg.loop,
                                subCfg.syncUnixSec
                        )
                );

                TimelineLog.logf("SUB-TL",
                        "started timeline dir=%s stepMs=%d loop=%s syncUnix=%d",
                        subCfg.timelineDir.getAbsolutePath(), subCfg.stepMs, subCfg.loop, subCfg.syncUnixSec);
            }
        }
    }
}
