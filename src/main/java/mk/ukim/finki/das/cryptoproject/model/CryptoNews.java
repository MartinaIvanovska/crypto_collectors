package mk.ukim.finki.das.cryptoproject.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "symbol_news")
public class CryptoNews {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private UUID uuid;

    @Column(nullable = false)
    private String symbol;

    @Column(nullable = false, length = 500)
    private String title;

    @Column(nullable = false)
    private String url;

    private String image;

    private String sentiment;

    private Double confidence;

    @Column(name = "scraped_at")
    private LocalDateTime scrapedAt;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    private String source;

    /* getters & setters */

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public UUID getUuid() { return uuid; }
    public void setUuid(UUID uuid) { this.uuid = uuid; }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getSentiment() { return sentiment; }
    public void setSentiment(String sentiment) { this.sentiment = sentiment; }

    public Double getConfidence() { return confidence; }
    public void setConfidence(Double confidence) { this.confidence = confidence; }

    public LocalDateTime getScrapedAt() { return scrapedAt; }
    public void setScrapedAt(LocalDateTime scrapedAt) { this.scrapedAt = scrapedAt; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
}
