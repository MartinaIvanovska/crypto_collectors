package mk.ukim.finki.das.cryptoproject.web;

import mk.ukim.finki.das.cryptoproject.model.Daily;
import mk.ukim.finki.das.cryptoproject.service.CryptoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import org.springframework.data.domain.Pageable;

@RequestMapping("/cryptos")
@Controller
public class CryptoController {

    @Autowired
    private CryptoService cryptoService;

    /**
     * List latest row per symbol (one row per symbol).
     * Supports pageable + sorting (symbol, date, close, volume).
     * Accepts pageNum (1-based) and pageSize as query parameters.
     */
    @GetMapping
    public String listLatest(
            @RequestParam(value = "pageNum", defaultValue = "1") int pageNum,
            @RequestParam(value = "pageSize", defaultValue = "20") int pageSize,
            Model model) {


        Pageable pageable = PageRequest.of(pageNum - 1, pageSize, Sort.by("symbol").ascending());
        Page<LatestDto> page = cryptoService.getLatestPerSymbol(pageable);
        model.addAttribute("page", page);

        return "list";
    }

    /**
     * Show full history for a single symbol.
     * Supports paging with pageNum (1-based) and pageSize.
     */
    @GetMapping("/{symbol:.+}")
    public String symbolHistory(
            @PathVariable("symbol") String symbol,
            @RequestParam(value = "pageNum", defaultValue = "1") int pageNum,
            @RequestParam(value = "pageSize", defaultValue = "30") int pageSize,
            Model model) {

        Pageable pageable = PageRequest.of(pageNum - 1, pageSize, Sort.by("date").descending());
        Page<Daily> page = cryptoService.getHistory(symbol, pageable);
        Daily latest = cryptoService.getLatestForSymbol(symbol);

        model.addAttribute("page", page);
        model.addAttribute("symbol", symbol);
        model.addAttribute("latest", latest);

        return "history"; //
    }

}
