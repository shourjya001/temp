import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class ApiDataFetcher {
    
    private final ObjectMapper objectMapper;

    public ApiDataFetcher() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
            JsonNode rootNode = objectMapper.readTree(json);
            List<ResponseInternalRatingsEvent> responseObjects = new ArrayList<>();

            if (rootNode.isArray()) {
                responseObjects = objectMapper.readValue(json, new TypeReference<List<ResponseInternalRatingsEvent>>() {});
            } else {
                ResponseInternalRatingsEvent singleEvent = objectMapper.treeToValue(rootNode, ResponseInternalRatingsEvent.class);
                responseObjects.add(singleEvent);
            }

            List<Relationship> allRelationships = new ArrayList<>();
            for (ResponseInternalRatingsEvent event : responseObjects) {
                if (event.getRelationships() != null) {
                    allRelationships.addAll(event.getRelationships());
                }
            }

            ResponseInternalRatingsEvent result = new ResponseInternalRatingsEvent();
            result.setRelationships(allRelationships);

            System.out.println("Total relationships processed: " + allRelationships.size());
            return result;
        } catch (IOException e) {
            System.err.println("Error processing JSON data: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
