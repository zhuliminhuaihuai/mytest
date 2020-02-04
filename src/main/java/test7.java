import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * test7
 *
 * @author {zhulimin}
 * @date 2020/2/3 0003 下午 17:23
 */
public class test7 {

        public static void main(String[] args) throws Exception {

            //获取执行环境
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


            //创建表的执行环境
            EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
            TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

            //文件格式 是读到的文件是什么格式的 输出就是什么格式的
            String filePath = "file:///C:/Users/Administrator/Documents/GitHub/mytest/src/main/resources/StudentGrand.csv";

            String outputPath1 = "file:///D:/办公/emsoft/flink/LevelA.csv";
            String outputPath2 = "file:///D:/办公/emsoft/flink/LevelB.csv";
            String outputPath3 = "file:///D:/办公/emsoft/flink/LevelC.csv";
            String outputPath4 = "file:///D:/办公/emsoft/flink/LevelF.csv";

            //注册一个TableSource
            CsvTableSource courseTable = CsvTableSource.builder()
                    .field("name", Types.STRING)
                    .field("fenshu", Types.DOUBLE)
                    .path(filePath)
                    .build();
            tableEnv.registerTableSource("courseTable", courseTable);

            //注册一个TableSink
            String[] fieldNames = {"name", "fenshu"};
            TypeInformation[] fieldTypes = {Types.STRING, Types.DOUBLE};

            /**
             * 1.输入地址
             * 2.输出字段名之间的分隔符
             * 3.设置输出文件个数
             * 4.设置文件是否覆盖
             */
            TableSink<Row> configure1 = new CsvTableSink(outputPath1, ",", 1, FileSystem.WriteMode.OVERWRITE);
            tableEnv.registerTableSink("tableSink1", fieldNames, fieldTypes, configure1);

            TableSink<Row> configure2 = new CsvTableSink(outputPath2, ",", 1, FileSystem.WriteMode.OVERWRITE);
            tableEnv.registerTableSink("tableSink2", fieldNames, fieldTypes, configure2);

            TableSink<Row> configure3 = new CsvTableSink(outputPath3, ",", 1, FileSystem.WriteMode.OVERWRITE);
            tableEnv.registerTableSink("tableSink3", fieldNames, fieldTypes, configure3);

            TableSink<Row> configure4 = new CsvTableSink(outputPath4, ",", 1, FileSystem.WriteMode.OVERWRITE);
            tableEnv.registerTableSink("tableSink4", fieldNames, fieldTypes, configure4);

//            //第一种做法
////            //sql查询 统计
////            Table resultTable1 = tableEnv.sqlQuery("select * from courseTable where fenshu >=90.0 and fenshu <=100");
////            Table resultTable2 = tableEnv.sqlQuery("select * from courseTable where fenshu >=75.0 and fenshu <90.0");
////            Table resultTable3 = tableEnv.sqlQuery("select * from courseTable where fenshu >=60.0 and fenshu <75.0");
////            Table resultTable4 = tableEnv.sqlQuery("select * from courseTable where fenshu <60.0");
////            // 将结果集 插入tableSink
////            resultTable1.insertInto("tableSink1");
////            resultTable2.insertInto("tableSink2");
////            resultTable3.insertInto("tableSink3");
////            resultTable4.insertInto("tableSink4");

            //第二种做法
            tableEnv.sqlUpdate("insert into tableSink1 select * from courseTable where fenshu >=90.0 and fenshu <=100");
            tableEnv.sqlUpdate("insert into tableSink2 select * from courseTable where fenshu >=75.0 and fenshu <90.0");
            tableEnv.sqlUpdate("insert into tableSink3 select * from courseTable where fenshu >=60.0 and fenshu <75.0");
            tableEnv.sqlUpdate("insert into tableSink4 select * from courseTable where fenshu <60.0");


            tableEnv.execute("start ");




        }

}
