import org.omg.CORBA.ARG_IN;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class DataParser
{
    SQLops SQL;
    private static int ThreadCount = 20;

    public DataParser(SQLops SQL)
    {
        this.SQL = SQL;
    }

    public void RunParse() throws SQLException, ExecutionException, InterruptedException, TimeoutException
    {
        ParsePageData(AggregateHistories());
    }
    private void ParsePageData(ArrayList<ArrayList<ArrayList<ArrayList<String>>>> AggregateHistory) throws ExecutionException, InterruptedException, TimeoutException, SQLException {
        ArrayList<ArrayList<String>> SQLPush = new ArrayList<>();
        System.out.println("Parsing...");
        for (int i = 0; i < AggregateHistory.size(); i++)
        {
            System.out.println("Page:"+i);
            if (AggregateHistory.get(i).size() > 1)
            {
                for (int j = 1; j < AggregateHistory.get(i).size()-1; j++)
                {
                    System.out.println("Time:"+j);
                    ArrayList<String> Offender = ContainsChanges(
                            new ArrayList<>(),AggregateHistory.get(i).get(j),AggregateHistory.get(i).get(j-1));
                    long CurrentTime = Timestamp.valueOf(AggregateHistory.get(i).get(j).get(0).get(8)).getTime();
                    long PreviousTime = Timestamp.valueOf(AggregateHistory.get(i).get(j-1).get(0).get(8)).getTime();
                    long DeltaTime = CurrentTime-PreviousTime;
                    String Batch = AggregateHistory.get(i).get(j).get(0).get(9);
                    while(Offender.size()>0)
                    {
                        if(SearchLeftandRight(i,j,Offender,AggregateHistory) == false)
                        {
                            System.out.println("New item found!! Batch:" + Batch);
                            SQL.InsertInto(SQL.SQLFatTrimmerData,new ArrayList<>(Arrays.asList("1",i,DeltaTime,Batch)));
                            break;
                        }
                        Offender = ContainsChanges(
                            Offender,AggregateHistory.get(i).get(j),AggregateHistory.get(i).get(j-1));
                    }
                    if(Offender.size()<1){
                        System.out.println("Old item found. Batch:" +Batch);
                        SQL.InsertInto(SQL.SQLFatTrimmerData, new ArrayList<>(Arrays.asList("0",i,DeltaTime,Batch)));
                    }
                }
            }
        }
    }
    private boolean SearchLeftandRight(int CurrentPage , int CurrentDepth, ArrayList<String> Offender, ArrayList<ArrayList<ArrayList<ArrayList<String>>>> AggregateHistory) throws ExecutionException, InterruptedException, TimeoutException {
        boolean Out = false;
        CompletableFuture L = CompletableFuture.supplyAsync(()-> (SearchLeft(CurrentPage,CurrentDepth,Offender,AggregateHistory)));
        CompletableFuture R = CompletableFuture.supplyAsync(()-> (SearchRight(CurrentPage,CurrentDepth,Offender,AggregateHistory)));
        while(Out == false)
        {
            if (L.isDone()){
                Out = (boolean) L.get(1, TimeUnit.SECONDS);
                if(Out){
                    R.cancel(true);
                    break;
                }
            }
            if (R.isDone()){
                Out = (boolean) R.get(1, TimeUnit.SECONDS);
                if(Out){
                    L.cancel(true);
                    break;
                }
            }
            if(L.isDone() && R.isDone())
            {
                break;
            }
        }
        return Out;
    }
    boolean SearchLeft(int CurrentPage,int CurrentDepth, ArrayList<String> Offender, ArrayList<ArrayList<ArrayList<ArrayList<String>>>> AggregateHistory)
    {
        while((CurrentPage-1)>-1)
        {
            if(ContainsItem(Offender,AggregateHistory.get(CurrentPage-1).get(CurrentDepth)))
                return true;
            CurrentPage = CurrentPage -1;
        }
        return false;
    }
    public boolean SearchRight(int CurrentPage ,int CurrentDepth, ArrayList<String> Offender, ArrayList<ArrayList<ArrayList<ArrayList<String>>>> AggregateHistory)
    {
        while((CurrentPage+1)<AggregateHistory.size())
        {
            if(ContainsItem(Offender,AggregateHistory.get(CurrentPage+1).get(CurrentDepth)))
                return true;
            CurrentPage = CurrentPage +1;
        }
        return false;
    }

    private boolean ContainsItem(ArrayList<String> Offender,ArrayList<ArrayList<String>> TestPage)
    {
        for (ArrayList<String> TestListing:TestPage)
        {
            if(Offender.get(0).equals(TestListing.get(0)) &&
                    Offender.get(1).equals(TestListing.get(1)) &&
                    Offender.get(3).equals(TestListing.get(3)))
            {
                return true;
            }
        }
        return false;
    }
    private ArrayList<ArrayList<ArrayList<String>>> GetPageHistory(int Page) throws SQLException
    {
        ArrayList<ArrayList<String>> InputSQL = GetPage(Page);
        LinkedHashSet<Integer> Batches = UniqueBatches(InputSQL);
        ArrayList<ArrayList<ArrayList<String>>> Out = new ArrayList<>();
        for (Integer Batch:Batches)
        {
            Out.add(new ArrayList<>(InputSQL.stream().filter(Listing -> Integer.parseInt(Listing.get(9)) == Batch)
                    .collect(Collectors.toList())));
        }
        return Out;
    }
    private LinkedHashSet<Integer> UniqueBatches(ArrayList<ArrayList<String>> InputData)
    {
        LinkedHashSet<Integer> Out = new LinkedHashSet<>();
        InputData.forEach(element -> Out.add(Integer.valueOf(element.get(9))));
        return Out;
    }
    private ArrayList<ArrayList<String>> GetPage(int Page) throws SQLException
    {
        return SQL.ResultSetTo2dArrayList(
                SQL.ExecuteQuery(
                        SQL.GetSQLContentsWithSearchConditionCommand(
                                SQL.SQLBazosDataTable, "Page",Page+" ORDER BY TimeOfRun ASC")),
                SQL.SQLBazosDataTableVarTypes.split(",").length);
    }
    private ArrayList<ArrayList<ArrayList<ArrayList<String>>>> AggregateHistories() throws SQLException {
        int MaxPage = Integer.parseInt(SQL.ResultSetRowToArrayList(SQL.ExecuteQuery(
                        "SELECT MAX(Page) AS MaximumPage FROM "+ SQL.SQLBazosDataTable+";")
                ,1).get(0));
        MaxPage = 10;//temp
        ArrayList<ArrayList<ArrayList<ArrayList<String>>>> Out = new ArrayList<>();
        System.out.println("Aggregating History:");
        for (int i = 0; i < MaxPage; i++)
        {
            Out.add(GetPageHistory(i));
            System.out.println("Page:"+i);
        }
        return Out;
    }
    private ArrayList<String> ContainsChanges(ArrayList<String> StartIterator, ArrayList<ArrayList<String>> Current, ArrayList<ArrayList<String>> Previous)
    {
        if(StartIterator.size()>0)
            Current = new ArrayList<>(Current.subList(Current.indexOf(StartIterator),Current.size()));

        for (int i = 0; i<Current.size(); i++)
        {
            ArrayList<String> CListing = Current.get(i);
            int Matching = Previous.stream().filter(PListing-> CListing.get(0).equals(PListing.get(0))
                    && CListing.get(1).equals(PListing.get(1))
                    && CListing.get(3).equals(PListing.get(3))).collect(Collectors.toList()).size();
            if(Matching<1)
                return CListing;
        }
        return new ArrayList<>();
    }
    private boolean isOld(ArrayList<ArrayList<String>> TotalData, ArrayList<String> CurrentData,int currentIndex)//returns number of new listings on the page
    {
        return TotalData.subList(0,currentIndex).stream().anyMatch(Listing ->
                Listing.get(0).equals(CurrentData.get(0))
                && Listing.get(1).equals(CurrentData.get(1))
                && Listing.get(3).equals(CurrentData.get(3)));
    }
    private ArrayList<String> CreateScrapeTask(int PagesPerThread,int Remainder,int PageCount)//Creates an Arraylist of limits for pages in the formant of Start,End  //should be working
    {
        ArrayList<String> OutArray = new ArrayList<>();
        int PageSetter = 0;
        int LoopLength = 1;
        int Counter = 0;
        String Filler ="";
        while(PageCount >= PageSetter)
        {
            if(PagesPerThread<=0){
                break;
            }
            if(PageSetter+PagesPerThread<=PageCount)
            {
                if(Counter!=LoopLength)
                {
                    Filler += PageSetter;
                }
                else{
                    Filler += PageSetter;
                    OutArray.add(Filler);
                    Counter = 0;
                    Filler = "";
                    Filler += PageSetter;
                }
                Filler += ",";
            }
            else{
                Filler += PageSetter+Remainder;
                OutArray.add(Filler);
                break;
            }
            Counter = Counter + 1;
            PageSetter = PageSetter+PagesPerThread;
        }
        return OutArray;
    }
}
