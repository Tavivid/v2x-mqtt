package org.example.v2x.common.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public final class Jsons {

    public static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private Jsons() {
    }
}
