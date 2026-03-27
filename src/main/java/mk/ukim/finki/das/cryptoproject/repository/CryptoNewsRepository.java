package mk.ukim.finki.das.cryptoproject.repository;

import mk.ukim.finki.das.cryptoproject.model.CryptoNews;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CryptoNewsRepository extends JpaRepository<CryptoNews, Long> {

    List<CryptoNews> findTop10BySymbolOrderByScrapedAtDesc(String symbol);
}
