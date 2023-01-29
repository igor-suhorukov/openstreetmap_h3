public class ImportCmd {
    public static void main(String[] args) {
        for(int i=0;i<50;i++){
            System.out.println("select count(*) from ways;");
            System.out.printf("copy ways from '/var/lib/heavyai/exchange/ways_%03d.tsv' with (header='false', delimiter='\\t', array_delimiter='|');\n",i);
        }
    }
}
