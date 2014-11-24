/**
 * This Java program exemplifies the basic usage of JDBC.  
 * Requirements:
 * (1) JDK 1.6+.
 * (2) SQLite3.
 * (3) SQLite3 JDBC jar (https://bitbucket.org/xerial/sqlite-jdbc/downloads/sqlite-jdbc-3.8.7.jar).
 */ 

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PA2
{
  public static void main (String[] args)
  {
    Connection conn = null; //Database connection


    try
    {
      // Load the JDBC class.
      Class.forName("org.sqlite.JDBC");
      // Get the connection to the database.
      // - "jdbc" : JDBC connection name prefix.
      // - "sqlite" : The concrete database implementation
      // (e.g., sqlserver, postgresql).
      // - "pa2.db" : The name of the database. In this project, we
      // use a local database named "pa2.db". This can also be a
      // remote database name
      conn = DriverManager.getConnection("jdbc:sqlite:pa2.db"); 
      System.out.println("Opened database successfully.");

      // Use case #1: Create and populate a table.
      // Get a Statement object.
      Statement stmt = conn.createStatement();
      stmt.executeUpdate("DROP TABLE IF EXISTS Flight;");
      stmt.executeUpdate("CREATE TABLE Flight(Airline, Origin, Destination);");
      stmt.executeUpdate("INSERT INTO Flight VALUES('AA','B','C'),('AA','C','D'), ('AA', 'A', 'B'), ('AA', 'D', 'E');");

      //Use case #2: Query the Flight table with Statement.
      //Returned query results are stored in a ResultSet object.
      ResultSet rset = stmt.executeQuery("SELECT * from Flight;");

      //Print the Origin and Destination columns.
      System.out.println("\nStatement Result:");
      // This shows how to traverse the ResultSet object.
      // Connected table
      stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
      stmt.executeUpdate("CREATE TABLE Connected(Airline, Origin, Destination, Stops INT);");
       
      // Connected = Flight
      stmt.executeUpdate("INSERT INTO Connected(Airline, Origin, Destination) SELECT * FROM Flight;");
      int count = 0;
      Statement dStmt = conn.createStatement();
      Statement cStmt = conn.createStatement();
      Statement tmpStmt = conn.createStatement();
      ResultSet delta = dStmt.executeQuery("SELECT * FROM Flight");
      // Create view
      tmpStmt.execute("DROP VIEW IF EXISTS DeltaView");
      tmpStmt.execute("CREATE VIEW DeltaView AS SELECT * FROM Connected WHERE Connected.Stops=" + count);
      // Semi-Naive Algorithm
      while(delta.next())
      {
        // 0 case
        if(count == 0) {
          // update Connected table
          dStmt.executeUpdate("INSERT INTO Connected SELECT *, " + count + " FROM Flight;");
          // delete initial NULL values from Flight that was copied into Connected
          tmpStmt.executeUpdate("DELETE FROM Connected WHERE Connected.Stops IS NULL");
          // verify contents of Connected
          ResultSet rs = tmpStmt.executeQuery("SELECT * FROM Connected");
          while(rs.next()) {
            System.out.print(rs.getString("Airline") + " ");
            System.out.print(rs.getString("Origin") + " ");
            System.out.print(rs.getString("Destination") + " ");
            System.out.print(rs.getString("Stops") + "\n");
          }
        }
        // general case
        else {
          // incorporate flight into this query
          dStmt.executeUpdate("INSERT INTO Connected SELECT x.Airline, x.Origin, y.Destination, " + count + " FROM DeltaView x, DeltaView y WHERE x.Destination=y.Origin");
          //ResultSet rs = tmpStmt.executeQuery("SELECT * FROM Connected");
          delta = dStmt.executeQuery("SELECT * FROM Connected WHERE Connected.Stops=" + count);
          ResultSet rs = delta;
          System.out.println("else on count: " + count);
          while(rs.next()) {
            System.out.print(rs.getString("Origin"));
            System.out.print("...");
            System.out.println(rs.getString("Destination") + " " + rs.getString("Stops"));
          }
        }
        // update delta ResultSet
        delta = dStmt.executeQuery("SELECT * FROM DeltaView");
        count++;
      }

      //Close the ResultSet and Statement objects.
      rset.close();
      stmt.close();

      delta.close();
      dStmt.close();
      cStmt.close();
    }
    catch (Exception e)
    {
      throw new RuntimeException("There was a runtime problem!", e);
    }
    finally
    {
      try
      {
        if(conn != null)
        {
          conn.close();
        }
      }
      catch (SQLException e)
      {
        throw new RuntimeException("Cannot close the connection!", e);
      }
    }
  }	
}
