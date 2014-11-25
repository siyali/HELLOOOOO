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

      // define the statements that I'll be using
      Statement cStmt = conn.createStatement(); // for the Connected table
      Statement pStmt = conn.createStatement(); // for the Prev table
      Statement dStmt = conn.createStatement(); // for the Delta table

      // create Connected table and fill with tuples from Flight
      stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
      stmt.executeUpdate("CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
      stmt.executeUpdate("INSERT INTO Connected(Airline, Origin, Destination) SELECT Airline, Origin, Destination FROM Flight;");

      int count = 0;

      // create Delta table and fill with tuples from Flight
      stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
      stmt.executeUpdate("CREATE TABLE Delta(Airline, Origin, Destination);");
      stmt.executeUpdate("INSERT INTO Delta(Airline, Origin, Destination) SELECT Airline, Origin, Destination FROM Flight;");

      ResultSet rsetDelta = stmt.executeQuery("SELECT * FROM Delta;");
      ResultSet rsetC = cStmt.executeQuery("SELECT * FROM Connected;");
      pStmt.executeUpdate("DROP TABLE IF EXISTS Prev;");
      pStmt.executeUpdate("CREATE TABLE Prev(Airline, Origin, Destination, Stops);");			
      
      while(rsetDelta.next())
      {
        if(count == 0)
        {
          cStmt.executeUpdate("INSERT INTO Connected SELECT *," + count + " FROM Flight;" );
          cStmt.executeUpdate("DELETE FROM Connected WHERE Connected.Stops IS NULL;");
        }
        else
        {
          // delete everything in Prev
          pStmt.executeUpdate("DELETE FROM Prev;");
          // now fill Prev with tuples from Connected
          pStmt.executeUpdate("INSERT INTO Prev SELECT * FROM Connected;");
          cStmt.executeUpdate("INSERT INTO Connected SELECT d.Airline, d.Origin, f.Destination,"+ count + " FROM  Flight f, Delta d WHERE f.Origin = d.Destination AND d.Airline = f.Airline AND d.Origin <> f.Destination;");
          rsetC = cStmt.executeQuery("SELECT * FROM Connected;");
          // delete everything in Delta
          dStmt.executeUpdate("DELETE  FROM Delta;");
          // now fill Delta with Connected - Prev which leaves us with the flights that have increased stopovers
          dStmt.executeUpdate("INSERT INTO Delta SELECT Airline, Origin, Destination FROM Connected EXCEPT SELECT Airline, Origin, Destination FROM Prev;");	
        }
        rsetDelta = stmt.executeQuery("SELECT * FROM Delta;");	
        count++;
      } 

      cStmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
      cStmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
      cStmt.executeUpdate("CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
      cStmt.executeUpdate("INSERT INTO Connected SELECT Airline, Origin, Destination, MIN(Stops) FROM Connected GROUP BY Airline, Origin, Destination ;");
      
      // Close the ResultSet and Statement objects.
      stmt.close();
      cStmt.close();
      dStmt.close();
      pStmt.close();
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
