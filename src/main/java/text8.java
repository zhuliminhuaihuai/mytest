import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * text8
 *
 * @author {zhulimin}
 * @date 2020/3/10 0010 下午 13:47
 */
public class text8 {

    public static void main(String[] args) throws Exception {


        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        //创建表的执行环境
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

        //文件格式 是读到的文件是什么格式的 输出就是什么格式的
        String filePath1 = "file:///D:/办公/emsoft/emsoft_flink/ceshi/jyjb.csv";
        String filePath2 = "file:///D:/办公/emsoft/emsoft_flink/ceshi/jyrz.csv";
        String filePath3 = "file:///D:/办公/emsoft/emsoft_flink/ceshi/nbzh.csv";
        String filePath4 = "file:///D:/办公/emsoft/emsoft_flink/ceshi/pz.csv";


        String outputPath1 = "file:///D:/办公/emsoft/flink/ceshi.csv";

        //注册交易基本表
        CsvTableSource jyjbTable = CsvTableSource.builder()
                .field("hxjylsbh", Types.STRING)
                .field("jyrq", Types.STRING)
                .field("rz", Types.STRING)
                .path(filePath1)
                .build();
        tableEnv.registerTableSource("jyjbTable", jyjbTable);

        //注册交易日志表
        CsvTableSource jyrzTable = CsvTableSource.builder()
                .field("jylsbh", Types.STRING)
                .field("jyrq", Types.STRING)
                .field("jydqdh", Types.STRING)
                .field("jyjgdh", Types.STRING)
                .field("jygydh", Types.STRING)
                .field("jydm", Types.STRING)
                .field("czbj", Types.STRING)
                .field("zhdh", Types.STRING)
                .field("sqgy", Types.STRING)
                .path(filePath2)
                .build();
        tableEnv.registerTableSource("jyrzTable", jyrzTable);

        //注册内部账户表
        CsvTableSource nbzhTable = CsvTableSource.builder()
                .field("zhdh", Types.STRING)
                .field("khmc", Types.STRING)
                .path(filePath3)
                .build();
        tableEnv.registerTableSource("nbzhTable", nbzhTable);

        //注册凭证值域表
        CsvTableSource pzTable = CsvTableSource.builder()
                .field("pzzl", Types.STRING)
                .field("pzmc", Types.STRING)
                .path(filePath4)
                .build();
        tableEnv.registerTableSource("pzTable", pzTable);

        String sql =
                "SELECT b.jyjgdh,b.jyrq,b.jygydh,b.sqgy,b.zhdh,b.khmc,pz.pzmc,b.qs,b.sl " +
                        "from " +
                        "(" +
                        "SELECT" +
                        "  a.*," +
                        "substring_index( a.s, '#', 1 ,1) AS sl, " +
                        " substring_index( a.s1, '#', 1,1 ) AS zl, " +
                        " substring_index( a.s2, '#', 1,1) AS qs " +
                        "  " +
                        "FROM " +
                        " ( " +
                        " SELECT " +
                        "   *, " +
                        "  substring_index( s.rz, ':',4 ,-1) AS s, " +
                        "  substring_index( s.rz, ':',3,-1 ) AS s1, " +
                        "  substring_index( s.rz, ':',2 ,-1) AS s2 " +
                        "  FROM (select rz.*,jb.rz,nb.khmc from jyrzTable rz join jyjbTable jb on rz.jylsbh=jb.hxjylsbh join nbzhTable nb on rz.zhdh = nb.zhdh where rz.jydm = '2410' and rz.czbj = 'chushou') as s  " +
                        " ) AS a" +
                        ") as b join pzTable pz on b.zl = pz.pzzl";

        String sql1 = "SELECT " +
                "   *, substring_index(s.rz,':',4,-1) AS s, substring_index(s.rz,':',3 ,-1) AS s1, substring_index(s.rz,':',2,-1) AS s2  " +
                "  FROM " +
                "   ( " +
                "   SELECT " +
                "    rz.*, " +
                "    jb.rz, " +
                "    nb.khmc  " +
                "   FROM " +
                "    jyrzTable rz " +
                "    JOIN jyjbTable jb ON rz.jylsbh = jb.hxjylsbh " +
                "    JOIN nbzhTable nb ON rz.zhdh = nb.zhdh  " +
                "   WHERE " +
                "    rz.jydm = 2410  " +
                "    AND rz.czbj = 'chushou'  " +
                "   ) AS s ";
        tableEnv.registerFunction("substring_index", new SubStringIndex());
        Table tableResult1 = tableEnv.sqlQuery(sql);
        //注册一个TableSink
        String[] fieldNames = {"jg", "jyrq", "gylsh", "sqgy", "zh", "hm", "pzmc", "qshm", "sl"};
        TypeInformation[] fieldTypes = {Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING};
        //注册输出表
        TableSink<Row> configure1 = new CsvTableSink(outputPath1, ",", 1, FileSystem.WriteMode.OVERWRITE);
        tableEnv.registerTableSink("tableSink1", fieldNames, fieldTypes, configure1);

        tableResult1.insertInto("tableSink1");

        tableEnv.execute("start");

//        System.out.println(new SubStringIndex().eval("#aaaa:1#aaaa:15#aaaa:2060013#aaaa:2060014#",":",4,-1));


    }
    public static class SubStringIndex extends ScalarFunction {
        /**
         * @param targetStr 目标字符串
         * @param str       查找字符串
         * @param index     第n次出现
         * @param order     顺序(大于0表示正序,小于0表示反序)
         * @return
         */
        public String eval(String targetStr, String str, int index, int order) {
            /**
             * 当 str 不存在于 targetStr 时，不管是正序还是反序都返回原字符串
             * 当index大于 str 在  targetStr 中出现的次数，不管是正序还是反序都返回原字符串
             */
            String result = targetStr;//默认返回字符串为原字符串
            if (targetStr == null || targetStr.trim().length() == 0) {
                return result;
            }
            //当index=0时，返回空
            if (index == 0) {
                return "";
            }
            //判断是正序还是反序（大于等于0表示正序，小于0表示反序）
            if (order < 0) {
                targetStr = new StringBuffer(targetStr).reverse().toString();
            }
            int beginIndex = 0;//用于匹配字符串的起始位置
            int count = 0; //记录字符串出现的次数
            while ((beginIndex = targetStr.indexOf(str, beginIndex)) != -1) {
                count++;
                //当index与字符串出现次数相同时，开始返回结果
                if (count == index) {
                    if (order < 0) {//反序时
                        targetStr = new StringBuffer(targetStr).reverse().toString();
                        result = targetStr.substring(targetStr.length() - beginIndex);
                    } else {//正序时
                        result = targetStr.substring(0, beginIndex);
                    }
                    return result;
                }
                beginIndex = beginIndex + str.length();//更改匹配字符串的起始位置
            }
            return result;
        }
    }

}
