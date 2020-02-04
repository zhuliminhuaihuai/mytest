import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * test5
 *
 * @author {zhulimin}
 * @date 2020/2/2 0002 下午 21:56
 */
public class test5 {
    public static void main(String[] args) throws Exception {

        //动态获取端口
        int port = 0;
        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.out.println("端口未设置，使用默认端口8080");
            port = 8080;
        }

        //获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("localhost", port);

        dataStreamSource.flatMap(new MyFlatMapFunction())
                //.keyBy("word")
                //keyby 选择器
                .keyBy(new KeySelector<wc1, Object>() {
                    @Override
                    public Object getKey(wc1 wc1) throws Exception {
                        return wc1.word;
                    }
                })
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print()
                .setParallelism(1);

        environment.execute("test4");
    }

    //自定义转换类
    public static class MyFlatMapFunction implements FlatMapFunction<String, wc1> {
        @Override
        public void flatMap(String value, Collector<wc1> collector) throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            for (String token : tokens
            ) {
                if (token.length() > 0) {
                    collector.collect(new wc1(token, 1));
                }
            }
        }
    }

    public static class wc1 {

        private String word;
        private int count;

        public wc1() {

        }

        public wc1(String word, int count) {
            this.word = word;
            this.count = count;
        }


        @Override
        public String toString() {
            return "wc{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }


    }
}
