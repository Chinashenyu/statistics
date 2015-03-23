import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.zdy.statistics.analysis.common.MysqlConnect;


public class Test {

	public static void main(String[] args){
		// TODO Auto-generated method stub
		Connection c1 = MysqlConnect.getConnection();
		Connection c2 = MysqlConnect.getConnection();
		
		System.out.println(c1 ==c2);
		
		try {
			PreparedStatement prepareStatement = c2.prepareStatement("");
			
			c1.close();
			
			prepareStatement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
