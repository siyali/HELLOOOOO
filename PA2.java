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

      // Use case #1: Create and populate a table.
      // Get a Statement object.
      Statement stmt = conn.createStatement();
      // Use case #2: Query the Flight table with Statement.
      // Returned query results are stored in a ResultSet object. 
      ResultSet rset = stmt.executeQuery("SELECT * FROM Flight;");

      // create Connected
      stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
      stmt.executeUpdate("CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
      stmt.executeUpdate("INSERT INTO Connected(Airline, Origin, Destination) SELECT Airline, Origin, Destination FROM Flight;");


      // set Connected to Flight
      int count = 0;

      stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
      stmt.executeUpdate("CREATE TABLE Delta(Airline, Origin, Destination);");
      stmt.executeUpdate("INSERT INTO Delta(Airline, Origin, Destination) SELECT Airline, Origin, Destination FROM Flight;");

      ResultSet rsetDelta = stmt.executeQuery("SELECT * FROM Delta;");
      Statement stmtC = conn.createStatement();
      ResultSet rsetC = stmtC.executeQuery("SELECT * FROM Connected;");
      Statement stmtDelta = conn.createStatement();
      Statement stmtP = conn.createStatement();
      stmtP.executeUpdate("DROP TABLE IF EXISTS Prev;");
      stmtP.executeUpdate("CREATE TABLE Prev(Airline, Origin, Destination, Stops);");			
      // TODO: reduce the amount of statement objects, they are causing a database lock!
      while(rsetDelta.next())
      {
        if(count == 0)
        {
          stmtC.executeUpdate("INSERT INTO Connected SELECT *," + count + " FROM Flight;" );
          stmtC.executeUpdate("DELETE FROM Connected WHERE Connected.Stops is NULL;");
        }
        else
        {	
          stmtP.executeUpdate("DELETE FROM Prev;");
          stmtP.executeUpdate("INSERT INTO Prev SELECT * FROM Connected;");
          stmtC.executeUpdate("INSERT INTO Connected SELECT d.Airline, d.Origin, f.Destination,"+ count + " FROM  Flight f, Delta d where f.Origin = d.Destination AND d.Airline = f.Airline AND d.Origin <> f.Destination  ;");
          rsetC = stmtC.executeQuery("SELECT * FROM Connected;");
          stmtDelta.executeUpdate("DELETE  FROM Delta;");
          stmtDelta.executeUpdate("INSERT INTO Delta SELECT Airline, Origin, Destination FROM Connected EXCEPT SELECT Airline, Origin, Destination FROM Prev;");	
        }
        rsetDelta = stmt.executeQuery("SELECT * FROM Delta;");	
        count++;
      } 

      stmtC.executeUpdate("INSERT INTO Connected SELECT Airline, Origin, Destination, MIN(Stops) FROM Connected GROUP BY Airline, Origin, Destination ;");
      
      // Close the ResultSet and Statement objects.
      stmt.close();
      stmtC.close();
      stmtDelta.close();
      stmtP.close();
      rset.close();
      rsetDelta.close();
      rsetC.close();
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
