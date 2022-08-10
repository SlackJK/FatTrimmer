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
        System.out.println(HistoryOfPages.size());
        for (int i = 0; i < HistoryOfPages.size(); i++)
        {
            if(i%100==0)
            {
                System.out.println(HistoryOfPages.get(i));
                System.out.println("Percentage complete: "+ (double) ((HistoryOfPages.size()*100/i)/100)+"%");
            }
            long CurrentTime =Timestamp.valueOf(HistoryOfPages.get(i).get(8)).getTime();
            String CurrentPage = HistoryOfPages.get(i).get(10);
            ArrayList<ArrayList<String>> OnlyThesePages = new ArrayList<>(HistoryOfPages.subList(0,i).stream()
                    .filter(element -> element.get(10).equals(CurrentPage))
                    .collect(Collectors.toList()));
            long PreviousTime = -1;
            if(OnlyThesePages.size()>0)
            {
                PreviousTime = Timestamp.valueOf(OnlyThesePages.get(OnlyThesePages.size()-1).get(8)).getTime();
            }
            long DeltaTime = CurrentTime-PreviousTime;
             if(isOld(HistoryOfPages,HistoryOfPages.get(i)))
             {
                 SQL.InsertInto(SQL.SQLFatTrimmerData,new ArrayList<>(Arrays.asList("0",CurrentPage,String.valueOf(DeltaTime))));
             }
             else{
                 SQL.InsertInto(SQL.SQLFatTrimmerData,new ArrayList<>(Arrays.asList("1",CurrentPage,String.valueOf(DeltaTime))));
             }

        }
        for (ArrayList DataRow:Out)
        {
            SQL.InsertInto(SQL.SQLFatTrimmerData,DataRow);
        }

    }
    private ArrayList<ArrayList<String>> GetPageHistory() throws SQLException
    {
        return SQL.ResultSetTo2dArrayList(SQL.ExecuteQuery("SELECT * FROM "+SQL.SQLBazosDataTable+" ORDER BY Batch ASC;"),
                SQL.SQLBazosDataTableVarTypes.split(",").length);

    }
    private boolean isOld(ArrayList<ArrayList<String>> TotalData, ArrayList<String> CurrentData)//returns number of new listings on the page
    {
        return TotalData.stream().anyMatch(Listing ->
                Listing.get(0).equals(CurrentData.get(0))
                && Listing.get(1).equals(CurrentData.get(1))
                && Listing.get(3).equals(CurrentData.get(3)));
    }
}
