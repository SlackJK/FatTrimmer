import javax.xml.crypto.Data;
import java.sql.SQLException;
import java.util.ArrayList;

public class MultiThread extends Thread
{
        private String InTask;
        private Thread t;
        private SQLops SQL;
        private DataParser DP;
        private ArrayList<ArrayList<String>> HistoryOfPages;
        public MultiThread(String Task, SQLops SQL, ArrayList<ArrayList<String>> HistoryOfPages)
        {
            InTask = Task;
            this.SQL = SQL;
            DP = new DataParser(SQL);
            this.HistoryOfPages = HistoryOfPages;
            System.out.println("Initializing...");
        }
        public void run()
        {
            String[] Task = InTask.split(",");
            for (int i = Integer.parseInt(Task[0]); i < Integer.parseInt(Task[1]); i++)
            {
                try {
                    DP.Parser(i,HistoryOfPages);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        public void start()
        {
            System.out.println("Starting thread: "+InTask);
            if(t == null)
            {
                t = new Thread(this,InTask);
                t.start();
            }
        }

}
