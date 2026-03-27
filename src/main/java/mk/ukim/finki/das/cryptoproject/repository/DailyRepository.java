package mk.ukim.finki.das.cryptoproject.repository;

import mk.ukim.finki.das.cryptoproject.model.Daily;
import mk.ukim.finki.das.cryptoproject.model.DailyId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DailyRepository extends JpaRepository<Daily, DailyId> {

    Page<Daily> findBySymbolOrderByDateDesc(String symbol, Pageable pageable);

    Daily findTopBySymbolOrderByDateDesc(String symbol);

    Page<Daily> findBySymbolContainingIgnoreCaseOrderByDateDesc(String symbol, Pageable pageable);
}
