package wdsr.exercise5.model;

import java.util.Date;

/**
 * @author Radek
 *
 */
public class Trade {

    private Integer id;

    private String asset;

    private Double amount;

    private Date date;

    public Trade(Integer id, String asset, Double amount, Date date) {
        this.id = id;
        this.asset = asset;
        this.amount = amount;
        this.date = date;
    }

    public Trade() {
		// TODO Auto-generated constructor stub
	}

	public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Trade trade = (Trade) o;

        if (id != null ? !id.equals(trade.id) : trade.id != null) return false;
        if (asset != null ? !asset.equals(trade.asset) : trade.asset != null) return false;
        if (amount != null ? !amount.equals(trade.amount) : trade.amount != null) return false;
        return date != null ? date.equals(trade.date) : trade.date == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (asset != null ? asset.hashCode() : 0);
        result = 31 * result + (amount != null ? amount.hashCode() : 0);
        result = 31 * result + (date != null ? date.hashCode() : 0);
        return result;
    }

	@Override
	public String toString() {
		return "Trade [id=" + id + ", asset=" + asset + ", amount=" + amount + ", date=" + date + "]";
	}
    
}
