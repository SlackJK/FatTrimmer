import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Main
{
    public static ConfigParser CFG = new ConfigParser("C:\\Users\\qsiba\\IdeaProjects\\bazos\\target\\config.cfg");//temp path for dev
    private static SQLops SQL = new SQLops();
    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException, TimeoutException
    {
        DataParser DP = new DataParser(SQL);
        DP.RunParse();
    }
}
