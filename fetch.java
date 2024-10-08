import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApiDataFetcher {
    
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
            ObjectMapper mapperObj = new ObjectMapper();
            mapperObj.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
            mapperObj.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            
            List<ResponseInternalRatingsEvent> responseObjects = mapperObj.readValue(json, 
                new TypeReference<List<ResponseInternalRatingsEvent>>() {});
            
            // Combine all relationships into a single list
            List<Relationship> allRelationships = new ArrayList<>();
            for (ResponseInternalRatingsEvent wrapper : responseObjects) {
                allRelationships.addAll(wrapper.getRelationships());
            }
            
            // Create a new object with all relationships
            ResponseInternalRatingsEvent transformedData = new ResponseInternalRatingsEvent();
            transformedData.setRelationships(allRelationships);
            
            return transformedData;
        } catch (IOException e) {
            System.err.println("Error processing JSON data: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
