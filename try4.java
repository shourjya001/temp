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
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressedBytes));
             InputStreamReader reader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8)) {
            return IOUtils.toString(reader);
        } catch (IOException e) {
            System.err.println("Error decompressing GZIP content: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private RobustApiDataFetcher processJsonData(String json) {
        ObjectMapper mapperObj = new ObjectMapper();
        mapperObj.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapperObj.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            // First, try to parse as a single RobustApiDataFetcher
            RobustApiDataFetcher singleObject = mapperObj.readValue(json, RobustApiDataFetcher.class);
            return singleObject;
        } catch (JsonProcessingException e) {
            // If parsing as a single object fails, try parsing as a list
            try {
                List<RobustApiDataFetcher> responseObjects = mapperObj.readValue(json, new TypeReference<List<RobustApiDataFetcher>>() {});
                
                // Combine all events into a single RobustApiDataFetcher
                RobustApiDataFetcher combinedResponse = new RobustApiDataFetcher();
                List<ResponseInternalRatingsEvent> allEvents = new ArrayList<>();
                for (RobustApiDataFetcher wrapper : responseObjects) {
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
