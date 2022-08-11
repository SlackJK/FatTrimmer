import org.omg.CORBA.ARG_IN;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

public class DataParser
{
    SQLops SQL;
    private static int ThreadCount = 20;

    public DataParser(SQLops SQL)
    {
        this.SQL = SQL;
    }
    public void ParsePageData() throws SQLException {
        int MaxPage = Integer.parseInt(SQL.ResultSetRowToArrayList(SQL.ExecuteQuery(
                "SELECT MAX(Page) AS MaximumPage FROM "+ SQL.SQLBazosDataTable+";")
                ,1).get(0));
        LabelNew(GetPageHistory());
    }
    private void LabelNew(ArrayList<ArrayList<String>> HistoryOfPages) throws SQLException
    {
        ArrayList<ArrayList<String>> Out = new ArrayList<>();
        HistoryOfPages.remove(0);
        System.out.println("Got data");
        int PageCount = HistoryOfPages.size();
        System.out.println(PageCount);
        int PagesPerThread = PageCount/ThreadCount;
        int Remainder = PageCount%ThreadCount;
        ArrayList<String> Task = CreateScrapeTask(PagesPerThread,Remainder,PageCount);
        ArrayList<MultiThread> Threads = new ArrayList<>();
        for (int i = 0; i < Task.size(); i++)
        {
            Threads.add(new MultiThread(Task.get(i),SQL,HistoryOfPages));
        }
        for (int i = 0; i < Threads.size(); i++)
        {
            Threads.get(i).start();
        }
    }
    public void Parser(int i,ArrayList<ArrayList<String>> HistoryOfPages) throws SQLException {
        if(i%100==0 && i!=0)
        {
            System.out.println(HistoryOfPages.get(i));
            //System.out.println("Listings parsed:"+i+"\nPercentage complete: "+  ((i/(double)HistoryOfPages.size())*100+"%"));
        }
        long CurrentTime =Timestamp.valueOf(HistoryOfPages.get(i).get(8)).getTime();
        String CurrentPage = HistoryOfPages.get(i).get(10);
        String CurrentBatch = HistoryOfPages.get(i).get(9);
        ArrayList<ArrayList<String>> OnlyThesePages = new ArrayList<>(HistoryOfPages.subList(0,i).stream()
                .filter(element -> element.get(10).equals(CurrentPage) && !element.get(9).equals(CurrentBatch))
                .collect(Collectors.toList()));
        long PreviousTime = -1;
        if(OnlyThesePages.size()>0)
        {
            PreviousTime = Timestamp.valueOf(OnlyThesePages.get(OnlyThesePages.size()-1).get(8)).getTime();
        }
        long DeltaTime = CurrentTime-PreviousTime;
        if(PreviousTime ==-1){
            DeltaTime = -1;
        }

        if(isOld(HistoryOfPages,HistoryOfPages.get(i),i))
        {
            SQL.InsertInto(SQL.SQLFatTrimmerData,new ArrayList<>(Arrays.asList("0",CurrentPage,String.valueOf(DeltaTime))));
        }
        else{
            SQL.InsertInto(SQL.SQLFatTrimmerData,new ArrayList<>(Arrays.asList("1",CurrentPage,String.valueOf(DeltaTime))));
        }
    }
    private ArrayList<ArrayList<String>> GetPageHistory() throws SQLException
    {
        System.out.println("Getting data");
        return SQL.ResultSetTo2dArrayList(SQL.ExecuteQuery("SELECT * FROM "+SQL.SQLBazosDataTable+" ORDER BY Batch ASC;"),
                SQL.SQLBazosDataTableVarTypes.split(",").length);

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
