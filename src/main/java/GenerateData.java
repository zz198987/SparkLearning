import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class GenerateData {
    public static void main(String[] args) throws IOException {
        String path = "F:/tmp/access.log";
        String os = System.getProperty("os.name");
        if(! os.toLowerCase().startsWith("win")){
            path = "/tmp/access.log";
        }
        if(args.length > 0)
        {
            path = args[0];
        }
        File file = new File(path);
        if(!file.exists())
        {
            file.createNewFile();
        }
        FileWriter fw = new FileWriter(file,true);
        PrintWriter printWriter = new PrintWriter(fw);
        try{
            while(true)
            {
                Thread.sleep(5000);
                for(int i =0;i< 100;i++)
                {
                    printWriter.println(getOneLine());
                }
                printWriter.flush();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            printWriter.close();
        }
    }

    public static String getOneLine()
    {
        String formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        return String.format("%s\t%s\t\"GET /%s HTTP/1.1\"\t%s\t%s",getIp(),formatDate,getCourse(),getHttpCode(),getUrl());

    }
    public static String getIp()
    {
        String[] ips = {"192","168","172","68","10","43","157","79"};
        return ips[new Random().nextInt(ips.length)] +"." +
                ips[new Random().nextInt(ips.length)] +"." +
                ips[new Random().nextInt(ips.length)] +"." +
                ips[new Random().nextInt(ips.length)];
    }

    public static String getCourse()
    {
        String[] course={"class","learn","course"};
        int index = new Random().nextInt(3);
        if(index == 2)
        {
            return "course/list";
        }

        return course[index]+"/"+new Random().nextInt(1000)+".html";
    }
    public static String getHttpCode()
    {
        String[] code = {"200","404","400","301","501"};
        return code[new Random().nextInt(code.length)];
    }

    public static String getUrl()
    {
        String[] searchEngines = {"baidu","google","sougou","bing","sousou","-"};
        int index = new Random().nextInt(searchEngines.length);
        if(index == 5)
        {
            return "-";
        }
        return "www."+searchEngines[index]+".com";
    }
}
