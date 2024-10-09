import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class ApiDataFetcher {

    public ResponseInternalRatingsEvent sendInternalRatingsEventsApi(String apiResponse, int statusCode) throws IOException {
        try {
            if (statusCode != 200) {
                throw new IOException("Unexpected status code: " + statusCode);
            }

            System.out.println("Successfully received data from API");
            
            // Decompress GZIP content
            byte[] compressedBytes = apiResponse.getBytes(StandardCharsets.ISO_8859_1);
            String decompressedJson = decompressGzipContent(compressedBytes);

            // Process JSON data
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            JsonNode rootNode = objectMapper.readTree(decompressedJson);
            List<ResponseInternalRatingsEvent> responseObjects = new ArrayList<>();

            if (rootNode.isArray()) {
                responseObjects = objectMapper.readValue(decompressedJson, new TypeReference<List<ResponseInternalRatingsEvent>>() {});
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
        } catch (JsonProcessingException e) {
            System.err.println("Error processing JSON: " + e.getMessage());
            throw new IOException("JSON processing error", e);
        } catch (JsonMappingException e) {
            System.err.println("Error mapping JSON to objects: " + e.getMessage());
            throw new IOException("JSON mapping error", e);
        }
    }

    private String decompressGzipContent(byte[] compressedBytes) throws IOException {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressedBytes));
             InputStreamReader reader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8)) {
            return IOUtils.toString(reader);
        }
    }
}
