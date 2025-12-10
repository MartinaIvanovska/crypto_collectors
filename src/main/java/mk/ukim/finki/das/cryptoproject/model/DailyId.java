package mk.ukim.finki.das.cryptoproject.model;

import java.io.Serializable;
import java.util.Objects;

public class DailyId implements Serializable {

    private String symbol;
    private String date;

    public DailyId() {}

    public DailyId(String symbol, String date) {
        this.symbol = symbol;
        this.date = date;
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DailyId)) return false;
        DailyId other = (DailyId) o;
        return Objects.equals(symbol, other.symbol) &&
                Objects.equals(date, other.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, date);
    }
}
