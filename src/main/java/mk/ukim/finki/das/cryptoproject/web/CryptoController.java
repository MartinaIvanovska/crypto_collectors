package mk.ukim.finki.das.cryptoproject.web;

import mk.ukim.finki.das.cryptoproject.model.Daily;
import mk.ukim.finki.das.cryptoproject.service.CryptoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/coins")
public class CryptoController {

    @Autowired
    private CryptoService service;

    // GET /api/coins?page=0&size=50&sort=volume,desc
    @GetMapping
    public Page<LatestDto> listLatest(@RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "50") int size,
                                      @RequestParam(required = false) String sort) {
        Sort s = Sort.by("symbol").ascending();
        if (sort != null && !sort.isBlank()) {
            String[] parts = sort.split(",");
            String prop = parts[0];
            Sort.Direction dir = parts.length > 1 && parts[1].equalsIgnoreCase("desc") ? Sort.Direction.DESC : Sort.Direction.ASC;
            s = Sort.by(dir, prop);
        }
        Pageable pageable = PageRequest.of(page, size, s);
        return service.getLatestPerSymbol(pageable);
    }

    // GET /api/coins/{symbol}
    @GetMapping("/{symbol}")
    public Daily latestForSymbol(@PathVariable String symbol) {
        return service.getLatestForSymbol(symbol);
    }

    // GET /api/coins/{symbol}/history?page=0&size=100
    @GetMapping("/{symbol}/history")
    public Page<Daily> history(@PathVariable String symbol,
                               @RequestParam(defaultValue = "0") int page,
                               @RequestParam(defaultValue = "100") int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "date"));
        return service.getHistory(symbol, pageable);
    }
}
