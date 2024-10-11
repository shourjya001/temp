import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class RobustApiDataFetcher {

    public ResponseWrapper fetchAndProcessData(int status, byte[] compressedBytes) {
        if (status != 200) {
            System.err.println("Unexpected status code: " + status);
            return null;
        }

        System.out.println("Successfully received data from Maestro");

        String decompressedJson = decompressData(compressedBytes);
        if (decompressedJson == null) {
            return null;
        }

        return processJsonData(decompressedJson);
    }

    private String decompressData(byte[] compressedBytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
             GZIPInputStream gzipInputStream = new GZIPInputStream(bais);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipInputStream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, len);
            }
            
            return outputStream.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            System.err.println("Error decompressing GZIP content: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private ResponseWrapper processJsonData(String json) {
        ObjectMapper mapperObj = new ObjectMapper();
        mapperObj.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapperObj.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            // First, try to parse as a single ResponseWrapper
            ResponseWrapper singleObject = mapperObj.readValue(json, ResponseWrapper.class);
            return singleObject;
        } catch (JsonProcessingException e) {
            // If parsing as a single object fails, try parsing as a list
            try {
                List<ResponseWrapper> responseObjects = mapperObj.readValue(json, new TypeReference<List<ResponseWrapper>>() {});
                
                // Combine all relationships into a single ResponseWrapper
                ResponseWrapper combinedResponse = new ResponseWrapper();
                List<ResponseInternalRatingsEvent> allEvents = new ArrayList<>();
                for (ResponseWrapper wrapper : responseObjects) {
                    if (wrapper.getResponseInternalRatingsEvents() != null) {
                        allEvents.addAll(wrapper.getResponseInternalRatingsEvents());
                    }
                }
                combinedResponse.setResponseInternalRatingsEvents(allEvents);
                return combinedResponse;
            } catch (JsonProcessingException e2) {
                System.err.println("Error parsing JSON: " + e2.getMessage());
                e2.printStackTrace();
                return null;
            }
        }
    }
