import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;



public class HiveJDBC {

/**
 * @param args
 * @throws SQLException
   */
private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

  public static void main(String[] args) throws SQLException {
      try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
   
//MySQL    
    
    String url="jdbc:mysql://localhost";
    Connection conn=DriverManager.getConnection(url,"hive","123456");
    Statement smt=conn.createStatement();
  
    String str="use test";
    String st="drop table maxime";
    String str0="create table maxime(serial int,num int,name VARCHAR(20))";
    String str1="insert into maxime(serial,num,name)values('1','1607','maxime')";
    String str2="insert into maxime(serial,num,name)values('2','1502','celine')";
    String str3="insert into maxime(serial,num,name)values('3','1715','eric')";
    
    String str4="select * from maxime into outfile '/tmp/test.txt'";
    
    smt.execute(str);
    smt.execute(st);
    smt.execute(str0);
    smt.execute(str1);
    smt.execute(str2);
    smt.execute(str3);
    smt.execute(str4);

  
//Hive  
    String user="hive";
    String password="123456";
    String hostname="localhost";
    String port="10000";
    String connString="jdbc:hive://" + hostname + ":" + port + "/default";
    Connection con=DriverManager.getConnection(connString , user , password);
    
    Statement stmt = con.createStatement();
   
    
    
    stmt.executeQuery("create table if not exists testHiveDriverTable(serial int,num int,name string)row format delimited fields terminated by '\t'");

    String sql1="select * from testhivedrivertable";
      
    stmt.executeQuery("load data local inpath '/tmp/test.txt' overwrite into table testhivedrivertable");
        
    System.out.println();
      
    ResultSet rs=stmt.executeQuery(sql1);
    java.sql.ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount=rsmd.getColumnCount();
    for(int i=1;i<=columnCount;i++){
   	  System.out.print(rsmd.getColumnName(i)+"\t");
    }
    System.out.println();
    while(rs.next())
    {
   	  System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getString(3));
    } 
  }
}