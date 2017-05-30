package wdsr.exercise5.dao;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import wdsr.exercise5.model.Trade;

@Repository
public class TradeDao {
	
	Logger logger = LogManager.getLogger(TradeDao.class);
	
	private final String SELECT_BY_ID = "SELECT * FROM trade WHERE id = ?";

    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * Zaimplementuj metode insertTrade aby wstawiała nowy rekord do tabeli "trade"
     * na podstawie przekazanego objektu klasy Trade.
     * @param trade
     * @return metoda powinna zwracać id nowego rekordu.
     */
    public int insertTrade(Trade trade) {
    	String sql = "INSERT INTO trade (asset, amount, date) VALUES (?, ?, ?)";
    	
    	KeyHolder keyHolder = new GeneratedKeyHolder();
    	jdbcTemplate.update(new PreparedStatementCreator() {
    		@Override
    	    public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
    	        PreparedStatement statement = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
    	        statement.setString(1, trade.getAsset());
    	        statement.setDouble(2, trade.getAmount());
    	        statement.setDate(3, new java.sql.Date(trade.getDate().getTime()));
    	        return statement;
    	    	}
    	    },keyHolder); 
    
    	return keyHolder.getKey().intValue();   
    }
    
    /**
     * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id.
     * Użyj intrfejsu RowMapper.
     * @param id
     * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym id.
     */
    public Optional<Trade> extractTrade(int id) {
    	Trade trade = jdbcTemplate.queryForObject(SELECT_BY_ID, new Object[] {id}, 
    			(rs, rowNum) -> {
    				Trade t = new Trade();
    				t.setAmount(rs.getDouble("amount"));
    				t.setAsset(rs.getString("asset"));
    				//t.setDate(date);
    				return t;
    			});
    	
    	if (trade == null) {
    		return Optional.empty();
    	}
    	logger.info(trade.toString());
    	return Optional.of(trade);
    }

    /**
     * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id.
     * @param id, rch - callback który przetworzy wyciągnięty wiersz.
     * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym id.
     */
    public void extractTrade(int id, RowCallbackHandler rch) {
    	String sql = SELECT_BY_ID;
    	jdbcTemplate.query(sql, new Object[] {id}, rch);
    }

    /**
     * Zaimplementuj metode aby zaktualizowała rekord o podanym id danymi z przekazanego parametru 'trade'
     * @param trade
     */
    public void updateTrade(int id, Trade trade) {
        String sql = "UPDATE trade SET asset =?, amount =? , date=? WHERE id= ?";
        Object[] params = new Object[] { trade.getAsset(), trade.getAmount(), trade.getDate(),id };
        int[] types = new int[] { Types.VARCHAR, Types.DOUBLE, Types.DATE,Types.INTEGER  };
        jdbcTemplate.update(sql, params, types);
    }

    /**
     * Zaimplementuj metode aby usuwała z bazy rekord o podanym id.
     * @param id
     */
    public void deleteTrade(int id) {
        String sql = "DELETE FROM trade WHERE id=?";
        jdbcTemplate.update(sql, new Object[] {id});
    }

}
