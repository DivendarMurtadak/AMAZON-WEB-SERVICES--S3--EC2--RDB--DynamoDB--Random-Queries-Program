import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.Date;
import java.util.Random;
import java.util.StringTokenizer;

public class RandomQuery2OnRDB{
  public static void main(String[] args) {
 System.out.println("Table Creation Example!");
 Connection con = null;

 try{
	 Random rn = new Random();
	 try {
         Class.forName("com.mysql.jdbc.Driver");
     } catch (ClassNotFoundException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
     } 
   long  lStartTime = new Date().getTime();
     System.out.println("start");
     con = DriverManager.getConnection("jdbc:mysql://localhost:3306/?","ankit","akdv1234");
     //con = DriverManager.getConnection("jdbc:mysql://ankitdeven.cury5aj8qlt4.us-west-2.rds.amazonaws.com:3306/innodb","ankit","akdv1234");
 // con = DriverManager.getConnection("jdbc:mysql://localhost:3306/innodb","ankit","akdv1234");
 try{///jdbc:mysql://localhost/jdbcdb////ankitdeven.cury5aj8qlt4.us-west-2.rds.amazonaws.com:3306
  Statement st = con.createStatement();
  
  String  sQuery= "";
  String sQuery_No= "SELECT COUNT(column_name) FROM STATION;";//Integer.parseInt(sQuery_No) 
  ResultSet rs = st.executeQuery(sQuery_No);
  int iCount=0;
  while (rs.next()) {
	   iCount = rs.getInt(0);
	  System.out.println(iCount + "\n");
	}
  System.out.println("start");
  
  int iPointOnePercent= (int) ((int)iCount*0.001);
  int iOnePercent=(int) ((int)iCount*0.01);
  int iRowNo=0;
  //Generating 25000 random queries  for 0.1 to 1 % of the data 
  for (int i = 0; i <=25000; i++) {
	  
	  int randomInt = rn.nextInt(1);
	  iRowNo= (int)randomInt*iCount;
	  if(iRowNo>=iPointOnePercent && iRowNo<=iOnePercent)
	  {
		  sQuery="SELECT top "+ iRowNo+"FROM weather ";
		   rs = st.executeQuery(sQuery);
	  }
	  else
	  {
		  i--;
	  }
}
//calculate time difference for update file time
	long lEndTime = new Date().getTime();
	long difference = lEndTime - lStartTime;
	System.out.println("Elapsed milliseconds: " + difference);
	System.out.println("Elapsed seconds: " + difference*0.001);
  
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