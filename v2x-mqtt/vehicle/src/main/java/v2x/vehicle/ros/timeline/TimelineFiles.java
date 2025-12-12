package v2x.vehicle.ros.timeline;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import v2x.vehicle.util.Jsons;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public final class TimelineFiles {

    private TimelineFiles() {}

    public static List<Path> listTimelineFiles(Path dir) {
        File[] files = dir.toFile().listFiles((d, name) -> name.endsWith(".json"));
        if (files == null) return List.of();
        return Arrays.stream(files)
                .map(File::toPath)
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .collect(Collectors.toList());
    }

    public static List<String> readRegionsFromJson(Path jsonPath) throws IOException {
        String text = Files.readString(jsonPath, StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) return List.of();

        // 形式1: ["cell-0000", ...]
        if (text.startsWith("[")) {
            String[] arr = Jsons.GSON.fromJson(text, String[].class);
            return Arrays.asList(arr);
        }

        // 形式2: {"regions":[...]}
        JsonObject obj = Jsons.GSON.fromJson(text, JsonObject.class);
        JsonElement e = obj.get("regions");
        if (e == null || !e.isJsonArray()) return List.of();

        JsonArray arr = e.getAsJsonArray();
        List<String> result = new ArrayList<>(arr.size());
        for (JsonElement je : arr) {
            if (je.isJsonPrimitive()) result.add(je.getAsString());
        }
        return result;
    }
}
