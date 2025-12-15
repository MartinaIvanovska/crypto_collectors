package mk.ukim.finki.das.cryptoproject.web;

public class LatestDto {
    private String symbol;
    private String date;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Long volume;
    private Long sourceTimestamp;

    // getters / setters
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
