package mk.ukim.finki.das.cryptoproject.service;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Map;

@Service
public class AnalyticsService {

    private final RestTemplate restTemplate = new RestTemplate();
    private final String API_URL = "http://python-service:8000/api";
    private final String ADMIN_URL = "http://python-service:8000/admin";

    // --- Existing methods adjusted to use generic Map/List types ---

    // Note: The /technical endpoint returns a JSON array. We use List<Map>
    public List<Map<String, Object>> getTechnicalAnalysis(String symbol) {
        String url = API_URL + "/technical/" + symbol;

        // Correctly deserialize List<Map<String, Object>> using exchange()
        ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
            url,
            HttpMethod.GET,
            null,
            new ParameterizedTypeReference<List<Map<String, Object>>>() {}
        );
        return response.getBody();
    }

    public Map<String, Object> getForecast(String symbol) {
        String url = API_URL + "/forecast/" + symbol;
        return restTemplate.getForObject(url, Map.class);
    }

    public Map<String, Object> getSentimentAndOnChain(String symbol, String keyword) {
        // ERROR LINE 1 FIXED: fromHttpUrl -> fromUriString
        String url = UriComponentsBuilder.fromUriString(API_URL + "/sentiment-onchain/" + symbol)
            .queryParam("keyword", keyword)
            .toUriString();

        return restTemplate.getForObject(url, Map.class);
    }

    // --- NEW methods using generic Map types ---

    // /onchain-metrics/{symbol} returns a single JSON object. Use Map.
    public Map<String, Object> getAggregatedOnChainMetrics(String symbol) {
        String url = API_URL + "/onchain-metrics/" + symbol;
        return restTemplate.getForObject(url, Map.class);
    }

    public Map<String, Object> getLatestWhaleMovements(Integer limit) {
        // ERROR LINE 2 FIXED: fromHttpUrl -> fromUriString
        String url = UriComponentsBuilder.fromUriString(API_URL + "/whale-reports")
            .queryParam("limit", limit)
            .toUriString();

        return restTemplate.getForObject(url, Map.class);
    }

    // --- Admin method remains the same ---

    public void triggerPipelineUpdate() {
        String url = ADMIN_URL + "/refresh-pipeline";
        restTemplate.postForLocation(url, null);
    }
}