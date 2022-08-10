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
        for (int i = 0; i < MaxPage+1; i++)
        {
            LabelNew(i);
        }
    }
    private void LabelNew(int Page) throws SQLException {
        ArrayList<ArrayList<ArrayList<String>>> PageHistory = GetPageHistory(Page);
        ArrayList<ArrayList<String>> Out = new ArrayList<>();
        if(PageHistory.size()>1)
        {
            for (int i = 1; i < PageHistory.size(); i++) {
                if(ContainsNew(PageHistory.get(i-1),PageHistory.get(i))>0)
                {
                    long CurrentTime = Timestamp.valueOf(PageHistory.get(i).get(0).get(8)).getTime();
                    long PreviousTime = Timestamp.valueOf(PageHistory.get(i-1).get(0).get(8)).getTime();
                    long DeltaTime = CurrentTime-PreviousTime;
                    Out.add(new ArrayList<>(Arrays.asList("1",String.valueOf(Page),String.valueOf(DeltaTime))));//HasNewData,PageNumber,DeltaTime
                }
                else{
                    long CurrentTime = Timestamp.valueOf(PageHistory.get(i).get(0).get(8)).getTime();
                    long PreviousTime = Timestamp.valueOf(PageHistory.get(i-1).get(0).get(8)).getTime();
                    long DeltaTime = CurrentTime-PreviousTime;
                    Out.add(new ArrayList<>(Arrays.asList("0",String.valueOf(Page),String.valueOf(DeltaTime))));//HasNewData,PageNumber,DeltaTime
                }
            }
        }
        for (ArrayList DataRow:Out)
        {
            SQL.InsertInto(SQL.SQLFatTrimmerData,DataRow);
        }
    }
    private ArrayList<ArrayList<ArrayList<String>>> GetPageHistory(int ForWhichPage) throws SQLException
    {
        ArrayList<ArrayList<String>> History = SQL.ResultSetTo2dArrayList(SQL.ExecuteQuery(
                SQL.GetSQLContentsWithSearchConditionCommand(
                SQL.SQLBazosDataTable,"Page",ForWhichPage+" ORDER BY TimeOfRun ASC")),
                SQL.SQLBazosDataTableVarTypes.split(",").length);
        LinkedHashSet<Long> Batches = new LinkedHashSet<>();
        History.forEach(element -> Batches.add(Long.valueOf(element.get(9))));
        ArrayList<ArrayList<ArrayList<String>>> Out = new ArrayList<>();
        for (Long Batch:Batches)
        {
               Out.add(new ArrayList<>(History.stream().filter(Listing -> Long.parseLong(Listing.get(9)) == Batch)
                       .collect(Collectors.toList())));
        }
        return Out;
    }
    private int ContainsNew(ArrayList<ArrayList<String>> PreviousPage, ArrayList<ArrayList<String>> CurrentPage)//returns number of new listings on the page
    {
        CurrentPage.removeAll(PreviousPage);
        return CurrentPage.size();
    }
}
