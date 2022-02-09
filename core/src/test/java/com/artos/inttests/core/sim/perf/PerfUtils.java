package com.artos.inttests.core.sim.perf;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;

public class PerfUtils {
    private static final String INDENT = "  ";
    private static final NumberFormat format = new DecimalFormat("#.##", DecimalFormatSymbols.getInstance(Locale.US));

    public static String toString(Map<String, Object> map) {
        return toString("", map);
    }

    private static String toString(String indent, Map<String, Object> map) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        builder.append("\n" + indent + "{\n");
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!(entry.getValue() instanceof Map) || !((Map) entry.getValue()).isEmpty()) {
                if (first)
                    first = false;
                else
                    builder.append(",\n");

                builder.append(indent + INDENT + "\"" + entry.getKey() + "\":");
                if (!(entry.getValue() instanceof Map))
                    builder.append("\"" + toString(entry.getValue()) + "\"");
                else
                    builder.append(toString(indent + INDENT, (Map<String, Object>) entry.getValue()));
            }
        }

        builder.append("\n" + indent + "}");
        return  builder.toString();
    }

    private static String toString(Object value) {
        if (value instanceof Double)
            return format.format(((Double) value).doubleValue());
        else
            return value.toString();
    }
}
