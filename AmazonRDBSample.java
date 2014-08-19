/* References:
 http://aws.amazon.com/apache2.0
 http://www.java-tips.org/other-api-tips/jdbc/import-data-from-txt-or-csv-files-into-mysql-database-t-3.html
 http://stackoverflow.com/questions/20279273/upload-csv-file-containing-million-values-to-a-particular-column-in-mysql
 http://stackoverflow.com/questions/3635166/how-to-import-csv-file-to-mysql-table
 http://viralpatel.net/blogs/java-load-csv-file-to-database/
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class AmazonRDBSample {

	public static void main(String[] args) throws Exception {
	        
System.out.println("Table Creation Example!");
 Connection con = null;

 try{
	 
	 try {
         Class.forName("com.mysql.jdbc.Driver");
         System.out.println("Class Loaded....");
	 } catch (ClassNotFoundException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
     } 
  
          
     con = DriverManager.getConnection("jdbc:mysql://Divendar.cury5aj8qlt4.us-west-2.rds.amazonaws.com:3306/innodb","cloud3","akdv1234");
     System.out.println("Connected to the database....");
     //con = DriverManager.getConnection("jdbc:mysql://Divendar.cury5aj8qlt4.us-west-2.rds.amazonaws.com:3306/innodb","Divendar","akdv1234");
  //con = DriverManager.getConnection("jdbc:mysql://localhost:3306/innodb","Divendar","akdv1234");
 try{
	 jdbc:mysql:localhost/jdbcdb////Divendar.cury5aj8qlt4.us-west-2.rds.amazonaws.com:3306
  Statement st = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
  String filename= "weather-100K.csv";
  BufferedReader br = new BufferedReader(new FileReader(filename));
	int iKey=1;
	Connection connection = new getConnection();
	Statement statement = con.createStatement();
	
	for (String line = br.readLine(); line != null; line = br.readLine()) {
	 System.out.println("Line:  "+ line);  //... // do stuff to file here  
		String sEachLine = line.toString();
		   String[] sTemp = sEachLine.split(Pattern.quote(","));
		   String query = " INSERT INTO `innodb`.`weather_100k` (`Id`, `STATION`, `STATION_NAME`, `DATE`, `TMAX`, `TMIN`, `TOBS`, `WDMV`, `AWND`, `WDF2`, `WDF5`, `WDFG`, `WSF2`, `WSF5`, `WSFG`, `PGTM`, `FMTM`) VALUES ("
		   		+iKey+", '"+sTemp[0]+"' , '"+sTemp[1]+"' , '"+sTemp[2]+"' , '"+sTemp[3]+"' , '"+sTemp[4]+"' , '"+sTemp[5]+"' , '"+sTemp[6]+"' , '"+sTemp[7]+"' , '"+sTemp[8]+"' , '"+sTemp[9]+"' , '"+sTemp[10]+"' , '"+sTemp[11]+"' , '"+sTemp[12]+"' , '"+sTemp[13]+"' , '"+sTemp[14]+"' , '"+sTemp[15]+"' );";
		    statement.addBatch(query);
		    if ((iKey) % 1000 == 0) {
		    	System.out.println("in executing batch");
                statement.executeBatch(); // Execute every 1000 items.
            }
		    System.out.println("Key value :"+iKey);
		    iKey++;
		    
	}
	statement.executeBatch();
	statement.close();
	con.close();
	
  String  sQuery=" INSERT INTO `innodb`.`weather_100k` (`Id`, `STATION`, `STATION_NAME`, `DATE`, `TMAX`, `TMIN`, `TOBS`, `WDMV`, `AWND`, `WDF2`, `WDF5`, `WDFG`, `WSF2`, `WSF5`, `WSFG`, `PGTM`, `FMTM`) VALUES ('1', 'test', 'test', '00000000', 'test', 'test', 'test', 'test', 'test', 'test', 'test', 'test', 'test', 'test', 'test', 'test', 'test');"; 
  st.executeUpdate(sQuery);

	
  }
  catch(SQLException s){
	  s.printStackTrace();
  System.out.println("Table all ready exists!");
  }
  con.close();
  }
  catch (Exception e){
  e.printStackTrace();
  }
  }
}
