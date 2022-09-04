import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Main
{
    public static ConfigParser CFG = new ConfigParser("C:\\Users\\qsiba\\IdeaProjects\\bazos\\target\\config.cfg");//temp path for dev
    private static SQLops SQL = new SQLops();
    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException, TimeoutException
    {

        ResultSet x = SQL.ExecuteQuery("SELECT * FROM bot.BazosData ORDER BY Batch DESC;");
        testParse(x);
        System.out.println("done");


        /*
        DataParser DP = new DataParser(SQL);
        DP.RunParse();

         */
    }
    private static void testParse(ResultSet x)
    {
        try {
            int size = SQL.SQLBazosDataTableVarTypes.split(",").length;
            HashMap<ArrayList<Integer>,Boolean> AllScrapes = new HashMap<>();
            HashMap<ArrayList<String>,ArrayList<Integer>> NewValues = new HashMap<>();
            HashMap<ArrayList<Integer>,Long> AllScrapesByTime = new HashMap<>();
            long row = 0;
            while(x.next())
            {
                ArrayList<String> z = resultSetRowToArrayList(x,size);
                if(z.get(size-4) != null) {
                    AllScrapesByTime.put(new ArrayList<>(Arrays.asList(Integer.valueOf(z.get(size - 2)), Integer.valueOf(z.get(size - 3)))), Timestamp.valueOf(z.get(size - 4)).getTime());
                    if (row%100000==0)
                    {
                        System.out.println("IS not Null");
                    }
                    NewValues.put(new ArrayList<>(Arrays.asList(z.get(0),z.get(1),z.get(3))),new ArrayList<>(Arrays.asList(Integer.valueOf(z.get(size - 2)), Integer.valueOf(z.get(size - 3)))));
                    AllScrapes.put(new ArrayList<>(Arrays.asList(Integer.valueOf(z.get(size - 2)), Integer.valueOf(z.get(size - 3)))), false);
                }
                if (row%100000==0)
                {
                    System.out.println(z.get(size - 2)+","+z.get(size-3));
                    System.out.println(row);
                }
                row = row + 1;
            }
            System.out.println(AllScrapes.size()+","+row);
            System.out.println(NewValues.size());
            Set<ArrayList<String>> KeySet = NewValues.keySet();
            KeySet.forEach(element-> AllScrapes.put(NewValues.get(element),true));
            System.out.println(AllScrapes);
            ArrayList<ArrayList<Integer>> Keys = new ArrayList<>(AllScrapes.keySet());
            Collections.sort(Keys, new Comparator<ArrayList<Integer>>() {
                @Override
                public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
                    return o1.get(0).compareTo(o2.get(0));
                }
            });
            ArrayList<ArrayList<Integer>> BatchHistoryPerPage = new ArrayList<>();
            for (ArrayList<Integer> Scrape:Keys)
            {
                if(BatchHistoryPerPage.size()>0)
                {
                    if(BatchHistoryPerPage.size()-1<Integer.valueOf(Scrape.get(0)))
                    {
                        BatchHistoryPerPage.add(new ArrayList<>(Arrays.asList(Integer.valueOf(Scrape.get(1)))));
                    }
                    else{
                        BatchHistoryPerPage.get(BatchHistoryPerPage.size()-1).add(Integer.valueOf(Scrape.get(1)));
                    }

                }
                else{
                    BatchHistoryPerPage.add(new ArrayList<>(Arrays.asList(Integer.valueOf(Scrape.get(1)))));
                }
            }
            System.out.println(BatchHistoryPerPage.size());
            for (int i = 0; i < BatchHistoryPerPage.size(); i++) {
                Collections.sort(BatchHistoryPerPage.get(i));

            }
            System.out.println(BatchHistoryPerPage.size());
            System.out.println(BatchHistoryPerPage.get(1));
            for (int i = 0; i < BatchHistoryPerPage.size(); i++)
            {
                for (int j = 0; j < BatchHistoryPerPage.get(i).size(); j++)//todo sorted in the wrong order sort the other direction ie DESC
                {
                    Integer currentBatch = BatchHistoryPerPage.get(i).get(j);
                    ArrayList<Integer> holder = new ArrayList<>(Arrays.asList(i,currentBatch));
                    long deltaTime = -1;
                    if(BatchHistoryPerPage.get(i).size()>(j+1)&&AllScrapesByTime.containsKey(holder))
                    {
                        Integer nextBatch = BatchHistoryPerPage.get(i).get(j+1);
                        ArrayList<Integer> nextHolder = new ArrayList<>(Arrays.asList(i,nextBatch));
                        deltaTime = AllScrapesByTime.get(nextHolder)-AllScrapesByTime.get(holder);
                    }
                    ArrayList temp = new ArrayList<>(Arrays.asList(AllScrapes.get(holder),i,deltaTime,currentBatch));
                    SQL.InsertInto(SQL.SQLFatTrimmerData,temp);
                }
            }
            //System.out.println(Keys);

        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
    private static ArrayList<String> resultSetRowToArrayList(ResultSet TurnMeIntoArrayList, int ResultSetCollumnCount) throws SQLException
    {
        ArrayList<String> SQLRow= new ArrayList<>();
        for (int i = 1; i < ResultSetCollumnCount+1; i++)
        {
            SQLRow.add(TurnMeIntoArrayList.getString(i));//todo might need to be set to object since not only varcahrs are used
        }
        return SQLRow;
    }

}
