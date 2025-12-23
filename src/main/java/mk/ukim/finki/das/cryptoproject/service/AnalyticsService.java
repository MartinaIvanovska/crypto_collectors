package mk.ukim.finki.das.cryptoproject.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.util.Map;

@Service
public class AnalyticsService {

    private final RestTemplate restTemplate = new RestTemplate();
    // In Docker, this hostname matches the service name in docker-compose
    private final String API_URL = "http://python-service:8000/api";

    public Object getTechnicalAnalysis(String symbol) {
        String url = API_URL + "/technical/" + symbol;
        return restTemplate.getForObject(url, Object[].class); // Return generic object or create DTO
    }

    public Object getForecast(String symbol) {
        String url = API_URL + "/forecast/" + symbol;
        return restTemplate.getForObject(url, Map.class);
    }

    public Object getSentimentAndOnChain(String symbol, String keyword) {
        String url = API_URL + "/sentiment-onchain/" + symbol + "?keyword=" + keyword;
        return restTemplate.getForObject(url, Map.class);
    }

    // Call this via a @Scheduled task daily
    public void triggerPipelineUpdate() {
        String url = "http://python-service:8000/admin/refresh-pipeline";
        restTemplate.postForLocation(url, null);
    }
}