import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class ApiDataFetcher {
    private final ObjectMapper mapperObj;

    public ApiDataFetcher() {
        mapperObj = new ObjectMapper();
        mapperObj.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapperObj.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(ResponseInternalRatingsEvent.class, new ResponseInternalRatingsEventDeserializer());
        mapperObj.registerModule(module);
    }

    public ResponseInternalRatingsEvent fetchData(Result result) {
        if (result.getStatus() == 200) {
            System.out.println("Successfully received data from Maestro");
            byte[] compressedBytes = result.getBody().getBytes(StandardCharsets.ISO_8859_1);
            String decompressedJson = decompressGzipContent(compressedBytes);
            
            if (decompressedJson != null) {
                return processJsonData(decompressedJson);
            }
        } else {
            System.err.println("Unexpected status code: " + result.getStatus());
        }
        return null;
    }
    
    private String decompressGzipContent(byte[] compressedBytes) {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressedBytes));
             InputStreamReader reader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8)) {
            return IOUtils.toString(reader);
        } catch (IOException e) {
            System.err.println("Error decompressing GZIP content: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
    
    private ResponseInternalRatingsEvent processJsonData(String json) {
        try {
            JsonNode rootNode = mapperObj.readTree(json);
            List<ResponseInternalRatingsEvent> responseObjects;

            if (rootNode.isArray()) {
                responseObjects = mapperObj.readValue(json, new TypeReference<List<ResponseInternalRatingsEvent>>() {});
            } else {
                ResponseInternalRatingsEvent singleEvent = mapperObj.treeToValue(rootNode, ResponseInternalRatingsEvent.class);
                responseObjects = List.of(singleEvent);
            }

            List<Relationship> allRelationships = new ArrayList<>();
            for (ResponseInternalRatingsEvent wrapper : responseObjects) {
                if (wrapper.getRelationships() != null) {
                    allRelationships.addAll(wrapper.getRelationships());
                }
            }

            ResponseInternalRatingsEvent transformedData = new ResponseInternalRatingsEvent();
            transformedData.setRelationships(allRelationships);

            System.out.println("Total relationships processed: " + allRelationships.size());
            return transformedData;
        } catch (IOException e) {
            System.err.println("Error processing JSON data: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private static class ResponseInternalRatingsEventDeserializer extends StdDeserializer<ResponseInternalRatingsEvent> {
        public ResponseInternalRatingsEventDeserializer() {
            this(null);
        }

        public ResponseInternalRatingsEventDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public ResponseInternalRatingsEvent deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            ResponseInternalRatingsEvent event = new ResponseInternalRatingsEvent();

            if (node.has("relationships") && node.get("relationships").isArray()) {
                event.setRelationships(jp.getCodec().treeToValue(node.get("relationships"), List.class));
            } else {
                event.setRelationships(new ArrayList<>());
            }

            if (node.has("reasons") && node.get("reasons").isArray()) {
                event.setReasons(jp.getCodec().treeToValue(node.get("reasons"), List.class));
            } else {
                event.setReasons(new ArrayList<>());
            }

            return event;
        }
    }
}
