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

      // the Statement and ResultSet instances I will be using
      Statement stmt = conn.createStatement();
      ResultSet rset = null;

      // create Curr table and fill with tuples from Flight
      stmt.executeUpdate("DROP TABLE IF EXISTS Curr;");
      stmt.executeUpdate("CREATE TABLE Curr(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
      stmt.executeUpdate("INSERT INTO Curr(Airline, Origin, Destination) SELECT Airline, Origin, Destination FROM Flight;");

      // create Delta table and fill with tuples from Flight
      stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
      stmt.executeUpdate("CREATE TABLE Delta(Airline, Origin, Destination);");
      stmt.executeUpdate("INSERT INTO Delta(Airline, Origin, Destination) SELECT Airline, Origin, Destination FROM Flight;");

      stmt.executeUpdate("DROP TABLE IF EXISTS Prev;");
      stmt.executeUpdate("CREATE TABLE Prev(Airline, Origin, Destination, Stops);");			
      rset = stmt.executeQuery("SELECT COUNT(*) FROM Delta;");
      // Returned query results are stored in a ResultSet object. 
      int count = 0; // used to update the count on stopovers
      int deltaSize = rset.getInt(1);
      while(deltaSize > 0) // while Delta is not empty do the following
      {
        if(count == 0)
        {
          stmt.executeUpdate("INSERT INTO Curr SELECT *," + count + " FROM Flight;" );
          stmt.executeUpdate("DELETE FROM Curr WHERE Curr.Stops IS NULL;");
        }
        else
        {
          // delete everything in Prev
          stmt.executeUpdate("DELETE FROM Prev;");
          // now fill Prev with tuples from Curr
          stmt.executeUpdate("INSERT INTO Prev SELECT * FROM Curr;");
          stmt.executeUpdate("INSERT INTO Curr SELECT d.Airline, d.Origin, f.Destination,"+ count + " FROM  Flight f, Delta d WHERE f.Origin = d.Destination AND d.Airline = f.Airline AND d.Origin <> f.Destination;");
          // delete everything in Delta
          stmt.executeUpdate("DELETE FROM Delta;");
          // now fill Delta with Curr - Prev which leaves us with the flights that have increased stopovers
          stmt.executeUpdate("INSERT INTO Delta SELECT Airline, Origin, Destination FROM Curr EXCEPT SELECT Airline, Origin, Destination FROM Prev;");	
        }
        rset = stmt.executeQuery("SELECT COUNT(*) FROM Delta;");
        deltaSize = rset.getInt(1);
        count++;
      } 

      // update the Connected table with the minimum stopovers
      stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
      stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
      stmt.executeUpdate("CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops INT);");
      stmt.executeUpdate("INSERT INTO Connected SELECT Airline, Origin, Destination, MIN(Stops) FROM Curr GROUP BY Airline, Origin, Destination ;");

      // Close the ResultSet and Statement objects.
      stmt.close();
      rset.close();
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
