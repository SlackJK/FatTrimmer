import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ConfigParser
{
    private FileReader FR;
    private Properties P = new Properties();

    public ConfigParser(String configFilePath)
    {
        try
        {
            FR = new FileReader(configFilePath);
            P.load(FR);
        }
        catch(IOException e){
            e.printStackTrace();
        }

    }
    public String getProperty(String property)
    {
        return P.getProperty(property);
    }

}
