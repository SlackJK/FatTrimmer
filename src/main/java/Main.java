import java.sql.SQLException;

public class Main
{
    public static ConfigParser CFG = new ConfigParser("C:\\Users\\qsiba\\IdeaProjects\\bazos\\target\\config.cfg");//temp path for dev
    private static SQLops SQL = new SQLops();
    public static void main(String[] args) throws SQLException
    {
        DataParser DP = new DataParser(SQL);
        DP.ParsePageData();
    }
}
