import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;


public class Test {

	public static void main(String[] args){
//		// TODO Auto-generated method stub
//		Connection c1 = MysqlConnect.getConnection();
//		Connection c2 = MysqlConnect.getConnection();
//		
//		System.out.println(c1 ==c2);
//		
//		try {
//			PreparedStatement prepareStatement = c2.prepareStatement("");
//			
//			c1.close();
//			
//			prepareStatement.executeQuery();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
		
//		Logger logger = Logger.getLogger(Test.class);
//		logger.debug("test test");
		
		System.out.println(DateTimeUtil.minuteCalculate(new Date(), 0));
	}

}
