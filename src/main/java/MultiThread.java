import java.util.ArrayList;

public class MultiThread extends Thread
{
        private String InTask;
        private Thread t;
        private int CurrentPage;
        private int CurrentDepth;
        private ArrayList<String> Offender;
        private ArrayList<ArrayList<ArrayList<ArrayList<String>>>> AggregateHistory;
        public MultiThread(String Task,int CurrentPage,int CurrentDepth, ArrayList<String> Offender, ArrayList<ArrayList<ArrayList<ArrayList<String>>>> AggregateHistory)
        {
            this.InTask = Task;
            this.CurrentPage = CurrentPage;
            this.CurrentDepth = CurrentDepth;
            this.Offender = Offender;
            this.AggregateHistory = AggregateHistory;
        }
        public void run()
        {
            if(InTask.contains("L"))
            {

            }
            if(InTask.contains("R"));
            {

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
