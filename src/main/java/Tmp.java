public class Tmp {

    public static void main(String[] args) {
        String s = "2019-04-15 23:03:25";
        String g =   "GET /class/264.html HTTP/1.1";
        //System.out.println(s.substring(0,4)+s.substring(5,7)+s.substring(8,10));
        int index = g.indexOf(".");
        System.out.println(g.substring(11,index));
    }
}
