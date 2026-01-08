package mk.ukim.finki.das.cryptoproject.service;

import mk.ukim.finki.das.cryptoproject.model.CryptoNews;
import mk.ukim.finki.das.cryptoproject.repository.CryptoNewsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CryptoNewsService {

    @Autowired
    private CryptoNewsRepository cryptoNewsRepository;

    public List<CryptoNews> getLatestNewsForSymbol(String symbol) {
        return cryptoNewsRepository.findTop10BySymbolOrderByScrapedAtDesc(symbol);
    }
}
