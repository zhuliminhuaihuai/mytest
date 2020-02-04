import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * test4
 *
 * @author {zhulimin}
 * @date 2020/2/2 0002 下午 21:19
 */
public class test4 {

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

        dataStreamSource.flatMap(new FlatMapFunction<String, wc>() {
            @Override
            public void flatMap(String value, Collector<wc> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens
                ) {
                    if (token.length() > 0) {
                        collector.collect(new wc(token, 1));
                    }
                }
            }
        })//.keyBy("word")
                //keyby 选择器
                .keyBy(new KeySelector<wc, Object>() {
                    @Override
                    public Object getKey(wc wc) throws Exception {
                        return wc.word;
                    }
                })
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print()
                .setParallelism(1);

        environment.execute("test4");
    }

    public static class wc {

        private String word;
        private int count;

        public wc() {

        }

        public wc(String word, int count) {
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

