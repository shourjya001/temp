import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class AdvancedApiDataFetcher {
    
    private static final int TIMEOUT = 300000; // 5 minutes
    private static final int MAX_RETRIES = 3;
    private static final String BASE_URL = "https://api.example.com/data"; // Replace with actual API URL
    
    public List<ResponseInternalRatingsEvent> fetchAllData() throws IOException {
        List<ResponseInternalRatingsEvent> allData = new ArrayList<>();
        int page = 1;
        boolean hasMoreData = true;
        
        while (hasMoreData) {
            String url = BASE_URL + "?page=" + page + "&pageSize=1000"; // Adjust page size as needed
            String json = fetchDataWithRetry(url);
            
            if (json != null) {
                List<ResponseInternalRatingsEvent> pageData = processJsonData(json);
                if (!pageData.isEmpty()) {
                    allData.addAll(pageData);
                    System.out.println("Fetched page " + page + ". Total records: " + allData.size());
                    page++;
                } else {
                    hasMoreData = false;
                }
            } else {
                hasMoreData = false;
            }
        }
        
        return allData;
    }
    
    private String fetchDataWithRetry(String url) throws IOException {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                return fetchData(url);
            } catch (IOException e) {
                System.err.println("Attempt " + attempt + " failed: " + e.getMessage());
                if (attempt == MAX_RETRIES) throw e;
            }
        }
        return null;
    }
    
    private String fetchData(String url) throws IOException {
        RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(TIMEOUT)
            .setConnectionRequestTimeout(TIMEOUT)
            .setSocketTimeout(TIMEOUT)
            .build();
        
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build()) {
            HttpGet request = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    byte[] compressedBytes = IOUtils.toByteArray(response.getEntity().getContent());
                    return decompressGzipContent(compressedBytes);
                } else {
                    throw new IOException("Unexpected status code: " + response.getStatusLine().getStatusCode());
                }
            }
        }
    }
    
    private String decompressGzipContent(byte[] compressedBytes) throws IOException {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressedBytes));
             InputStreamReader reader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8)) {
            return IOUtils.toString(reader);
        }
    }
    
    private List<ResponseInternalRatingsEvent> processJsonData(String json) throws IOException {
        List<ResponseInternalRatingsEvent> results = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        
        try (JsonParser parser = mapper.getFactory().createParser(json)) {
            if (parser.nextToken() == JsonToken.START_ARRAY) {
                while (parser.nextToken() == JsonToken.START_OBJECT) {
                    ResponseInternalRatingsEvent event = mapper.readValue(parser, ResponseInternalRatingsEvent.class);
                    results.add(event);
                }
            }
        }
        
        return results;
    }
    
    public static void main(String[] args) {
        try {
            AdvancedApiDataFetcher fetcher = new AdvancedApiDataFetcher();
            List<ResponseInternalRatingsEvent> allData = fetcher.fetchAllData();
            System.out.println("Total records fetched: " + allData.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
