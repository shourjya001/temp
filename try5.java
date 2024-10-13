import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.zip.Inflater;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

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
    headers.set("Accept-Encoding", "identity");
    
    HttpEntity<String> entity = new HttpEntity<>("", headers);
    
    ResponseEntity<String> result = restTemplate.exchange(
        this.dbeclientProperties.getMaestroLebdrIdApiUrl() + maestrodate,
        HttpMethod.GET,
        entity,
        String.class
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
        byte[] compressedBytes = result.getBody().getBytes(StandardCharsets.ISO_8859_1);
        String decompressedJson = decompressData(compressedBytes);
        
        if (decompressedJson == null) {
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
            System.out.println(transformedJson);
            
            responseObject = mapperObj.readValue(transformedJson, ResponseInternalRatingsEvent.class);
        } catch (JsonProcessingException e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
            e.printStackTrace();
        }
    } else {
        System.err.println("Unexpected status code: " + status);
    }
    
    System.out.println(responseObject);
    return responseObject;
}

private String decompressData(byte[] compressedBytes) {
    try {
        Inflater inflater = new Inflater(true);
        inflater.setInput(compressedBytes);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressedBytes.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        inflater.end();

        byte[] decompressedBytes = outputStream.toByteArray();
        return new String(decompressedBytes, StandardCharsets.UTF_8);
    } catch (Exception e) {
        System.err.println("Error decompressing content: " + e.getMessage());
        e.printStackTrace();
        return null;
    }
}
