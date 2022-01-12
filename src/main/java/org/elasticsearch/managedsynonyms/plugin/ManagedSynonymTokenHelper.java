package org.elasticsearch.managedsynonyms.plugin;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public final class ManagedSynonymTokenHelper {

    public static String normalize(String term) {
        return term.replaceAll("[\n\r,]", "").toLowerCase().trim();
    }

    public static String getErrorMessageFromResponse(AcknowledgedResponse response) {
        try {
            var bytes = new BytesStreamOutput();
            response.writeTo(bytes);

            var jFactory = new JsonFactory();
            var parser = jFactory.createParser(new ByteArrayInputStream(bytes.bytes().array()));

            var asMap = convertParserToMap(parser);

            var errorReason = getMappedValueAsString(asMap, "error.reason");
            if (errorReason != null) return errorReason;

            var rootCauseReason = getMappedValueAsString(asMap, "error.root_cause.reason");
            if (rootCauseReason != null) return rootCauseReason;

            var errorType = getMappedValueAsString(asMap, "error.type");
            if (errorType != null) return errorType;

            return "Unknown error";
        } catch (IOException e) {
            return "IOException occured when trying to parse error response: " + e.getMessage();
        }
    }

    public static String getMappedValueAsString(Map<String, Object> map, String path) {
        return (String) getMappedValue(map, path, null);
    }

    public static Object getMappedValueAsString(Map<String, Object> map, String path, String defaultValue) {
        return (String) getMappedValue(map, path, defaultValue);
    }

    public static Object getMappedValue(Map<String, Object> map, String path) {
        return getMappedValue(map, path, null);
    }

    @SuppressWarnings("unchecked")
    public static Object getMappedValue(Map<String, Object> map, String path, Object defaultValue) {
        var pathParts = path.split("\\.");
        var currentMap = map;
        int pathIndex = 0;
        while (pathIndex < pathParts.length) {
            if (!currentMap.containsKey(pathParts[pathIndex])) return defaultValue;
            var pathValue = currentMap.get(pathParts[pathIndex]);
            if (pathIndex == (pathParts.length - 1)) {
                return pathValue;
            }

            if (pathValue instanceof Map<?, ?>) {
                currentMap = (Map<String, Object>) pathValue;
                pathIndex++;
                continue;
            }

            return defaultValue;
        }
        return defaultValue;
    }

    public static Map<String, Object> convertParserToMap(JsonParser parser) throws IOException {
        var retMap = new HashMap<String, Object>();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            var token = parser.currentToken();
            if (JsonToken.FIELD_NAME.equals(token)) {
                var fieldName = parser.currentName();
                token = parser.nextToken();
                if (JsonToken.START_OBJECT.equals(token)) {
                    retMap.put(fieldName, convertParserToMap(parser));
                } else if (JsonToken.START_ARRAY.equals(token)) {
                    retMap.put(fieldName, getParserArrayValue(parser));
                } else {
                    if (token.isNumeric()) {
                        if (JsonToken.VALUE_NUMBER_INT.equals(token)) {
                            retMap.put(fieldName, parser.getValueAsInt());
                        } else if (JsonToken.VALUE_NUMBER_FLOAT.equals(token)) {
                            retMap.put(fieldName, parser.getValueAsDouble());
                        }
                    }
                }
            }
        }
        return retMap;
    }

    private static List<Object> getParserArrayValue(JsonParser parser) throws IOException {
        var retArray = new ArrayList<Object>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            var token = parser.currentToken();
            if (JsonToken.START_OBJECT.equals(token)) {
                retArray.add(convertParserToMap(parser));
            } else if (JsonToken.START_ARRAY.equals(token)) {
                retArray.add(getParserArrayValue(parser));
            } else {
                if (token.isNumeric()) {
                    if (JsonToken.VALUE_NUMBER_INT.equals(token)) {
                        retArray.add(parser.getValueAsInt());
                    } else if (JsonToken.VALUE_NUMBER_FLOAT.equals(token)) {
                        retArray.add(parser.getValueAsDouble());
                    }
                } else if (token.isBoolean()) {
                    retArray.add(parser.getValueAsBoolean());
                } else {
                    retArray.add(parser.getValueAsString());
                }
            }
        }
        return retArray;
    }

    public static String getMapAsJsonString(Map<String, Object> map) throws IOException {
        try (StringWriter writer = new StringWriter()) {
            var jFactory = new JsonFactory();
            var builder = jFactory.createGenerator(writer);

            builder.writeStartObject();
            buildMapAsString(map, builder);
            builder.writeEndObject();
            builder.flush();
            return writer.toString();
        }
    }

    private static void buildMapAsString(Map<String, Object> map, JsonGenerator builder) throws IOException {
        for (var entry : map.entrySet()) {
            builder.writeFieldName(entry.getKey());
            writeMapValueToBuilder(entry.getValue(), builder);
        }
    }

    @SuppressWarnings("unchecked")
    private static void writeMapValueToBuilder(Object value, JsonGenerator builder) throws IOException {
        if (value instanceof Map) {
            builder.writeStartObject();
            buildMapAsString((Map<String, Object>) value, builder);
            builder.writeEndObject();
        } else if (value instanceof List) {
            builder.writeStartArray();
            for (var item : (List<?>) value) {
                writeMapValueToBuilder(item, builder);
            }
            builder.writeEndArray();
        } else if (value instanceof Boolean) {
            builder.writeBoolean((Boolean) value);
        } else if (value instanceof Integer) {
            builder.writeNumber((Integer) value);
        } else if (value instanceof Long) {
            builder.writeNumber((Long) value);
        } else if (value instanceof Float) {
            builder.writeNumber((Float) value);
        } else if (value instanceof Double) {
            builder.writeNumber((Double) value);
        } else {
            builder.writeString(value.toString());
        }
    }
}
