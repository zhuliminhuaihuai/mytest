import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;


/**
 * test6
 *
 * @author {zhulimin}
 * @date 2020/2/3 0003 上午 10:07
 */
public class test6 {



    public static void main(String[] args) throws Exception {

        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //创建表的执行环境
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

        String filePath = "file:///D:/办公/emsoft/flink/aaa.csv";

        String outputPath = "file:///D:/办公/emsoft/flink/bbb.csv";

        //注册一个TableSource
        CsvTableSource build = CsvTableSource.builder()
                .ignoreFirstLine()
                .field("id", Types.STRING)
                .field("userId", Types.STRING)
                .field("cishu", Types.STRING)
                .field("pay", Types.DOUBLE)
                .path(filePath)
                .build();
        tableEnv.registerTableSource("sales", build);

        //注册一个TableSink
        String[] fieldNames = {"userId", "money"};
        TypeInformation[] fieldTypes = {Types.STRING, Types.DOUBLE};
        TableSink<Row> configure = new CsvTableSink(outputPath, "|", 1, FileSystem.WriteMode.OVERWRITE);

        tableEnv.registerTableSink("tableSink", fieldNames, fieldTypes, configure);

        //sql查询 统计
        Table resultTable = tableEnv.sqlQuery("select userId,sum(pay) money from sales group by userId");

        System.out.println(resultTable.toString());

        resultTable.insertInto("tableSink");

        tableEnv.execute("start ");



        //读取csv文件 获取数据源
//        DataSource<Sales> csv = env.readCsvFile(filePath)
//                .ignoreFirstLine()
//                .pojoType(Sales.class, "id", "userId", "cishu", "pay");


        //把DataSet 转换成Table
//        Table sales = tableEnv.fromDataSet(csv);

        //注册表
//        tableEnv.registerTable("sales", sales);

        //sql查询 统计 返回结果集也是一个Table
//        Table resultTable = tableEnv.sqlQuery("select userId,sum(pay) money from sales group by userId");
//
//        DataSet<Row> result = tableEnv.toDataSet(resultTable, Row.class);
//
//        result.print();

//        salesDataSource.print();

    }

    public static class Sales {
        public String id;
        public String userId;
        public String cishu;
        public Double pay;

        public Sales() {
        }

        public Sales(String id, String userId, String cishu, Double pay) {
            this.id = id;
            this.userId = userId;
            this.cishu = cishu;
            this.pay = pay;
        }

        @Override
        public String toString() {
            return "Sales{" +
                    "id='" + id + '\'' +
                    ", userId='" + userId + '\'' +
                    ", cishu='" + cishu + '\'' +
                    ", pay=" + pay +
                    '}';
        }
    }
}
