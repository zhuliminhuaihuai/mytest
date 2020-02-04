import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * test1 批处理
 *
 * @author {zhulimin}
 * @date 2020/1/30 0030 下午 16:43
 */
public class test1 {

    public static void main(String[] args) throws Exception {

        String input = "file:///C:/Users/Administrator/Documents/GitHub/aaa.txt";

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> stringDataSource = environment.readTextFile(input);

//        stringDataSource.print();
        stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\t");
                for (String token : tokens
                ) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();

    }


}
