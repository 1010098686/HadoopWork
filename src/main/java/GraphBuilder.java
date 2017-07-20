/**
 * Created by rajesh on 17-7-15.
 */
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
public class GraphBuilder {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private Text character = new Text();
        private Text out = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] tmp = str.split("\t");
            character.set(tmp[0]);
            out.set("1" + "\t" + tmp[1]);
            context.write(character, out);
        }
    }


}
