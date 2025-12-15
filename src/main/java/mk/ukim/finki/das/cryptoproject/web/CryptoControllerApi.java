package mk.ukim.finki.das.cryptoproject.web;

import mk.ukim.finki.das.cryptoproject.model.Daily;
import mk.ukim.finki.das.cryptoproject.service.CryptoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/coins")
public class CryptoControllerApi {

    @Autowired
    private CryptoService cryptoService;

    /**
     * Returns historical OHLCV data for a symbol, suitable for Plotly candlestick chart.
     *
     * Example: GET /api/coins/KERNEL-USD/history/candlestick?limit=100
     */
    @GetMapping("/{symbol}/history/candlestick")
    public Map<String, Object> getCandlestickHistory(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "100") int limit) {

        // Sort by date ascending (oldest first)
        Pageable pageable = PageRequest.of(0, limit, Sort.by(Sort.Direction.ASC, "date"));
        Page<Daily> historyPage = cryptoService.getHistory(symbol, pageable);

        // Convert data to lists
        List<String> dates = historyPage.stream().map(d -> d.getDate().toString()).toList();
        List<Double> open = historyPage.stream().map(Daily::getOpen).toList();
        List<Double> high = historyPage.stream().map(Daily::getHigh).toList();
        List<Double> low = historyPage.stream().map(Daily::getLow).toList();
        List<Double> close = historyPage.stream().map(Daily::getClose).toList();
        List<Long> volume = historyPage.stream().map(Daily::getVolume).toList();

        // Prepare JSON response
        Map<String, Object> response = new HashMap<>();
        response.put("dates", dates);
        response.put("open", open);
        response.put("high", high);
        response.put("low", low);
        response.put("close", close);
        response.put("volume", volume);

        return response;
    }
}
