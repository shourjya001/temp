import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.zip.Inflater;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class ApiDataFetcher {
    private List<Map<String, Object>> relationships;

    public List<Map<String, Object>> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<Map<String, Object>> relationships) {
        this.relationships = relationships;
    }

    public ApiDataFetcher fetchAndProcessData(int status, byte[] compressedBytes) {
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

    private ApiDataFetcher processJsonData(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String, Object>> dataList = mapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});

            List<Map<String, Object>> allRelationships = new ArrayList<>();
            for (Map<String, Object> item : dataList) {
                if (item.containsKey("relationships")) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> rels = (List<Map<String, Object>>) item.get("relationships");
                    allRelationships.addAll(rels);
                } else {
                    allRelationships.add(item);
                }
            }

            ApiDataFetcher result = new ApiDataFetcher();
            result.setRelationships(allRelationships);
            return result;
        } catch (Exception e) {
            System.err.println("Error processing JSON: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
