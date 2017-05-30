import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.jdbc.JdbcTestUtils;
import wdsr.exercise5.dao.TradeDao;
import wdsr.exercise5.model.Trade;

import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;

/**
 * Created by Mateusz on 2017-05-08.
 */
@ContextConfiguration("/spring-context.xml")
@RunWith(value = SpringRunner.class)
public class TradeDaoTest {
	
	Logger logger = LogManager.getLogger(TradeDaoTest.class);

    @Autowired
    TradeDao tradeDao;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    public void insertTradeTest() {
        // given
        Trade trade = new Trade(1, "$ASIA_TOP_50", 100.0, Calendar.getInstance().getTime());

        // when
        int i = tradeDao.insertTrade(trade);

        // then
        Assert.assertEquals(2, i);
    }

    @Test
    public void updateTradeTest() {
        // given
        Date date = java.sql.Date.valueOf(LocalDate.of(2017, 5, 1));
        Trade trade = new Trade(null, "PKP INTERCITY", 70.0, date);

        // when
        tradeDao.updateTrade(1, trade);

        // then
        Assert.assertEquals(1, JdbcTestUtils.countRowsInTableWhere(jdbcTemplate, "trade", "id=1 and asset='PKP INTERCITY' and amount=70.0 and date='2017-05-01'"));
    }

    @Test
    public void extractTradeTest() {
        // given
        Date date = java.sql.Date.valueOf(LocalDate.of(2017, 1, 1));
        Trade expectedTrade = new Trade(0, "GFT", 90.0, date);

        // when
        Optional<Trade> trade = tradeDao.extractTrade(0);

        // then
        Assert.assertTrue(trade.isPresent());
        logger.info(trade.get().toString());
        Assert.assertEquals(expectedTrade, trade.get());
    }

    @Test
    public void extractTradeWithCallbackTest() {
        // given
        Date date = java.sql.Date.valueOf(LocalDate.of(2017, 1, 1));
        Trade expectedTrade = new Trade(0, "GFT", 90.0, date);

        final Trade[] actualTrade = new Trade[1];
        RowCallbackHandler rch = rs -> {
            actualTrade[0] = new Trade(rs.getInt(1), rs.getString(2), rs.getDouble(3), rs.getDate(4));
            Assert.assertEquals(expectedTrade, actualTrade[0]);
        };

        // when
        tradeDao.extractTrade(0, rch);

        // then
        Assert.assertNotNull(actualTrade[0]);
    }

    @Test
    public void deleteTradeTest() {
        // given

        // when
        tradeDao.deleteTrade(0);

        // then
        Assert.assertEquals(2, JdbcTestUtils.countRowsInTable(jdbcTemplate, "trade"));
        Assert.assertEquals(0, JdbcTestUtils.countRowsInTableWhere(jdbcTemplate, "trade", "id=0"));

    }

}
