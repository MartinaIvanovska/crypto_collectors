package mk.ukim.finki.das.cryptoproject.service;

import mk.ukim.finki.das.cryptoproject.model.Daily;
import mk.ukim.finki.das.cryptoproject.repository.DailyRepository;
import mk.ukim.finki.das.cryptoproject.web.LatestDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CryptoService {

    @Autowired
    private DailyRepository dailyRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Returns a Page of latest row per symbol (one row per symbol)
     */
    public Page<LatestDto> getLatestPerSymbol(Pageable pageable) {
        Integer total = jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT symbol) FROM daily", Integer.class);
        if (total == null) total = 0;

        // Build simple order by
        String orderBy = "d.symbol ASC";
        if (pageable.getSort().isSorted()) {
            Sort.Order order = pageable.getSort().iterator().next();
            String prop = order.getProperty();
            String dir = order.isAscending() ? "ASC" : "DESC";
            if ("volume".equalsIgnoreCase(prop) || "close".equalsIgnoreCase(prop) || "date".equalsIgnoreCase(prop) || "symbol".equalsIgnoreCase(prop)) {
                orderBy = "d." + prop + " " + dir;
            }
        }

        String sql = "SELECT d.symbol, d.date, d.open, d.high, d.low, d.close, d.volume, d.source_timestamp " +
                "FROM daily d " +
                "INNER JOIN (SELECT symbol, MAX(date) AS max_date FROM daily GROUP BY symbol) latest " +
                "ON d.symbol = latest.symbol AND d.date = latest.max_date " +
                "ORDER BY " + orderBy + " LIMIT ? OFFSET ?";

        int limit = pageable.getPageSize();
        int offset = (int) pageable.getOffset();

        List<LatestDto> content = jdbcTemplate.query(sql,
                new Object[]{limit, offset},
                new BeanPropertyRowMapper<>(LatestDto.class));

        return new PageImpl<>(content, pageable, total);
    }

    public Page<Daily> getHistory(String symbol, Pageable pageable) {
        return dailyRepository.findBySymbolOrderByDateDesc(symbol, pageable);
    }

    public Daily getLatestForSymbol(String symbol) {
        return dailyRepository.findTopBySymbolOrderByDateDesc(symbol);
    }
}
