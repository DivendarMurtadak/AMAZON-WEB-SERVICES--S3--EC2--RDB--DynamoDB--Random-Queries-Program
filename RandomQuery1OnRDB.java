import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.Date;
import java.util.Random;
import java.util.StringTokenizer;

public class RandomQuery1OnRDB{
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
     con = DriverManager.getConnection("jdbc:mysql://localhost:3306/?","Divendar","akdv1234");
     
 try{
  Statement st = con.createStatement();
  
  String  sQuery= "";
  String sQuery_No= "SELECT COUNT(column_name) FROM STATION;";
  ResultSet rs = st.executeQuery(sQuery_No);
  int iCount=0;
  while (rs.next()) {
	   iCount = rs.getInt(0);
	  System.out.println(iCount + "\n");
	}
 // st.executeUpdate(sQuery);
  System.out.println("start");
  //Generating 50000 random queries
  for (int i = 0; i <=50000; i++) {
	  
	  int randomInt = rn.nextInt(1);
	  sQuery="SELECT top "+ (int)randomInt*iCount+"FROM weather ";//select top 1 field
	   rs = st.executeQuery(sQuery);
	  
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
