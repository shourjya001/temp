import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.zip.Inflater;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;


private String decompressData(byte[] compressedBytes) {
    // First, try GZIP decompression
    try {
        ByteArrayInputStream bis = new ByteArrayInputStream(compressedBytes);
        GZIPInputStream gis = new GZIPInputStream(bis);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = gis.read(buffer)) != -1) {
            bos.write(buffer, 0, len);
        }
        gis.close();
        bos.close();
        return new String(bos.toByteArray(), StandardCharsets.UTF_8);
    } catch (IOException e) {
        System.out.println("GZIP decompression failed, trying Inflater: " + e.getMessage());
    }

    // If GZIP fails, try Inflater
    try {
        Inflater inflater = new Inflater(true); // true for ZLIB header
        inflater.setInput(compressedBytes);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressedBytes.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        inflater.end();

        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    } catch (DataFormatException | IOException e) {
        System.err.println("Error decompressing content with Inflater: " + e.getMessage());
        e.printStackTrace();
    }

    // If both decompression methods fail, return the original data as a string
    System.out.println("Both decompression methods failed. Returning original data as string.");
    return new String(compressedBytes, StandardCharsets.UTF_8);
}

public ResponseInternalRatingsEvent sendInternalRatingsEventsApi() throws IOException, JsonException {
    String access_token = generateSGconnectToken();
    ResponseInternalRatingsEvent responseObject = null;
    String maestrodate = "?snapshotDate=2024-09-11";
    
    System.out.println("*sending Data To ApT****");
    
    RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.set("Authorization", "Bearer " + access_token);
    headers.set("content-Language", "en-US");
    headers.set("Host", "maestro-search-uat.fr.world.socgen");
    headers.set("Accept", "*/*");
    headers.set("content-type", "application/json");
    headers.set("accept", "application/octet-stream");
    headers.set("Accept-Encoding", "gzip, deflate");
    
    HttpEntity<String> entity = new HttpEntity<>("", headers);
    
    ResponseEntity<byte[]> result = restTemplate.exchange(
        this.dbeclientProperties.getMaestroLebdrIdApiUrl() + maestrodate,
        HttpMethod.GET,
        entity,
        byte[].class
    );
    
    int status = result.getStatusCode().value();
    
    if (status == 400 || status == 401 || status == 402 || status == 403 || status == 404 || status == 500 || status == 201) {
        String errorMessage = "API returned status code: " + status;
        System.err.println(errorMessage);
        sendSgmrDataService.sendErrorNotification("API Error", errorMessage);
        return null;
    }
    
    if (status == 200) {
        System.out.println("Successfully Data received from Maestro");
        byte[] responseBody = result.getBody();
        String decompressedJson = decompressData(responseBody);
        
        if (decompressedJson == null) {
            System.err.println("Failed to decompress or read the response data");
            return null;
        }
        
        try {
            ObjectMapper mapperObj = new ObjectMapper();
            mapperObj.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
            mapperObj.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            
            List<ResponseInternalRatingsEvent> responseObjects = mapperObj.readValue(decompressedJson, 
                new TypeReference<List<ResponseInternalRatingsEvent>>() {});
            
            List<Relationships> allRelationships = new ArrayList<>();
            for (ResponseInternalRatingsEvent wrapper : responseObjects) {
                if (wrapper.getRelationships() != null) {
                    allRelationships.addAll(wrapper.getRelationships());
                }
            }
            
            ResponseInternalRatingsEvent transformedData = new ResponseInternalRatingsEvent();
            transformedData.setRelationships(allRelationships);
            
            String transformedJson = mapperObj.writeValueAsString(transformedData);
            System.out.println("Transformed JSON: " + transformedJson);
            
            responseObject = mapperObj.readValue(transformedJson, ResponseInternalRatingsEvent.class);
        } catch (JsonProcessingException e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
            e.printStackTrace();
        }
    } else {
        System.err.println("Unexpected status code: " + status);
    }
    
    System.out.println("Response object: " + responseObject);
    return responseObject;
}
