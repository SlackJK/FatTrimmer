import java.sql.*;
import java.util.ArrayList;

public class SQLops
{
    String connectionUrl = Main.CFG.getProperty("connectionUrl");
    String SQLBazosDataTable = Main.CFG.getProperty("SQLBazosDataTable");
    String SQLBazosDataTableVarTypes= Main.CFG.getProperty("SQLBazosDataTableVarTypes");
    Connection con;
    public SQLops()
    {
        System.setProperty("oracle.jdbc.ReadTimeout", "5000");
        System.setProperty("oracle.jdbc.javaNetNio", "false");
        try
        {
            // Load SQL Server JDBC driver and establish connection.
            System.out.print("Connecting to SQL Server ... ");
            try (Connection con = DriverManager.getConnection(connectionUrl))
            {
                this.con = DriverManager.getConnection(connectionUrl);
                System.out.println("Connection Established.");
            }
        }
        catch (Exception e)
        {
            System.out.println("Connection Failed:");
            e.printStackTrace();
        }
    }
    public String GetSQLContentsWithSearchConditionCommand(String InWhatTable, String WhereColumn, String EqualsWhat)//when inputting string in EqualsWhat you need to add '' around it.
    {
        return "SELECT * FROM "+InWhatTable+" WHERE "+WhereColumn+" = "+EqualsWhat+";";
    }
    public ResultSet ExecuteQuery(String Command) throws SQLException
    {
        Statement statement = con.createStatement();
        return statement.executeQuery(Command);
    }
    public ArrayList<String> ResultSetRowToArrayList(ResultSet TurnMeIntoArrayList, int ResultSetCollumnCount) throws SQLException
    {
        TurnMeIntoArrayList.next();
        ArrayList<String> SQLRow= new ArrayList<>();
        for (int i = 1; i < ResultSetCollumnCount+1; i++)
        {
            SQLRow.add(TurnMeIntoArrayList.getString(i));//todo might need to be set to object since not only varcahrs are used
        }
        return SQLRow;
    }
    /*
    Turns a sql row into an ArrayList
    ---IMPORTANT---
    Private supporting method for grabbing multiple SQL rows can not be used for single line grabs
    MUST USE CORRECT COLUMN COUNT FOR CORRESPONDING RESULT SET
     */
    private ArrayList<String> resultSetRowToArrayList(ResultSet TurnMeIntoArrayList, int ResultSetCollumnCount) throws SQLException
    {
        ArrayList<String> SQLRow= new ArrayList<>();
        for (int i = 1; i < ResultSetCollumnCount+1; i++)
        {
            SQLRow.add(TurnMeIntoArrayList.getString(i));//todo might need to be set to object since not only varcahrs are used
        }
        return SQLRow;
    }
    /*
    Turns SQL table into 2dArrayList
    DOES NOT PRESERVE COLUMN NAMES
    All values become Strings
     */
    public ArrayList<ArrayList<String>> ResultSetTo2dArrayList(ResultSet TurnAllOfMeInto2dArrayList, int ResultSetCollumnCount) throws SQLException
    {
        ArrayList<ArrayList<String>> Out2dArrayList= new ArrayList<ArrayList<String>>();
        while (TurnAllOfMeInto2dArrayList.next())
        {
            Out2dArrayList.add(resultSetRowToArrayList(TurnAllOfMeInto2dArrayList,ResultSetCollumnCount));
        }
        return Out2dArrayList;
    }
}
