/**
 * Created by andy on 16-10-17.
 */
public class test {
public static String log = "2016-10-17T18: 46:17.053881+08:00 open1 Apache  - [aliopen1@001 tag=\"Aliopen1Test\"] 211.103.193.66 - - [17/Oct/2016:18:46:15 +0800] \"GET /templates/tz_jollyness_joomla/favicon.ico HTTP/1.1\" 200 2105";
    public static void main(String[] args){
        String[] log1 = log.split(" ");
        //System.out.println(log1.length);
        for(int i=0;i<log1.length;i++){
            System.out.println(log.split(" ")[i]);
        }
        //System.out.println(log.split(" ")[10]);
    }
}
