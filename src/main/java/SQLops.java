import java.sql.*;
import java.util.ArrayList;

public class SQLops
{
    String connectionUrl = Main.CFG.getProperty("connectionUrl");
    String SQLBazosDataTable = Main.CFG.getProperty("SQLBazosDataTable");
    String SQLBazosDataTableVarTypes= Main.CFG.getProperty("SQLBazosDataTableVarTypes");
    String SQLFatTrimmerData = Main.CFG.getProperty("SQLFatTrimmerData");
    String SQLFatTrimmerDataVarTypes= Main.CFG.getProperty("SQLFatTrimmerDataVarTypes");
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
                CheckIfTableExists(SQLFatTrimmerData,SQLFatTrimmerDataVarTypes);
                System.out.println("Connection Established.");
            }
        }
        catch (Exception e)
        {
            System.out.println("Connection Failed:");
            e.printStackTrace();
        }
    }
    private void CheckIfTableExists(String Table,String VarTypesIfDoesnt) throws SQLException
    {
        Statement statement = con.createStatement();
        String testintializetablequery  = "IF NOT EXISTS(SELECT object_id, * FROM sys.objects WHERE type = 'U' AND name = '"+Table+"') CREATE TABLE "+Table+"( "+VarTypesIfDoesnt+" );";
        statement.executeUpdate(testintializetablequery);
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
    public void InsertInto(String Table, ArrayList<Object> Values) throws SQLException
    {
        String QuestionMarks ="";
        for (int i = 0; i < Values.size(); i++)
        {
            if(i!=Values.size()-1) {
                QuestionMarks += "?,";
            }
            else{
                QuestionMarks += "?";
            }
        }
        String Insert = "INSERT INTO "+Table+" VALUES ("+QuestionMarks+");";
        PreparedStatement st = con.prepareStatement(Insert);
        for (int i = 1; i < Values.size()+1; i++)
        {
            st.setObject(i,Values.get(i-1));
        }
        st.executeUpdate();
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
        System.out.println("Converting");
        int i = 0;
        System.out.println(TurnAllOfMeInto2dArrayList.getFetchSize());
        while (TurnAllOfMeInto2dArrayList.next())
        {
            if(i%100000==0 && i!=0)
            {
                System.out.println("Rows Converted:"+i);
            }
            Out2dArrayList.add(resultSetRowToArrayList(TurnAllOfMeInto2dArrayList,ResultSetCollumnCount));
            i = i +1;
        }
        System.out.println("Finished conversion");
        return Out2dArrayList;
    }
}
