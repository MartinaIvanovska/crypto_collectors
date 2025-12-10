package mk.ukim.finki.das.cryptoproject.model;

import jakarta.persistence.*;

@Entity
@IdClass(DailyId.class)
@Table(name = "daily")
public class Daily {

    @Id
    @Column(name = "symbol")
    private String symbol;

    @Id
    @Column(name = "date")
    private String date;

    @Column(name = "open")
    private Double open;

    @Column(name = "high")
    private Double high;

    @Column(name = "low")
    private Double low;

    @Column(name = "close")
    private Double close;

    @Column(name = "volume")
    private Long volume;

    @Column(name = "source_timestamp")
    private Long sourceTimestamp;

    public Daily() {}

    // getters + setters ...

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public Double getOpen() { return open; }
    public void setOpen(Double open) { this.open = open; }

    public Double getHigh() { return high; }
    public void setHigh(Double high) { this.high = high; }

    public Double getLow() { return low; }
    public void setLow(Double low) { this.low = low; }

    public Double getClose() { return close; }
    public void setClose(Double close) { this.close = close; }

    public Long getVolume() { return volume; }
    public void setVolume(Long volume) { this.volume = volume; }

    public Long getSourceTimestamp() { return sourceTimestamp; }
    public void setSourceTimestamp(Long sourceTimestamp) { this.sourceTimestamp = sourceTimestamp; }
}
