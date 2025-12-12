package mk.ukim.finki.das.cryptoproject.web;

import mk.ukim.finki.das.cryptoproject.model.Daily;
import mk.ukim.finki.das.cryptoproject.service.CryptoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
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
     * Supports pageable + sorting (the service only accepts simple sorts: symbol, date, close, volume).
     */
    @GetMapping
    public String listLatest(
            @PageableDefault(size = 20, sort = "symbol", direction = Sort.Direction.ASC) Pageable pageable,
            Model model) {

        var page = cryptoService.getLatestPerSymbol(pageable);
        model.addAttribute("page", page);
        return "list";
    }

    /**
     * Show full history for a single symbol.
     */
    @GetMapping("/{symbol:.+}")
    public String symbolHistory(
            @PathVariable("symbol") String symbol,
            @PageableDefault(size = 30, sort = "date", direction = Sort.Direction.DESC) Pageable pageable,
            Model model) {

        Page<Daily> page = cryptoService.getHistory(symbol, pageable);
        Daily latest = cryptoService.getLatestForSymbol(symbol);

        model.addAttribute("page", page);
        model.addAttribute("symbol", symbol);
        model.addAttribute("latest", latest);
        return "history";
    }

}
