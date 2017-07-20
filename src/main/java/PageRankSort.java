/**
 * Created by rajesh on 17-7-12.
 */
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class PageRankSort {

    public static class MyMapper extends Mapper<Object, Text, FloatWritable, Text> {
        private Text page = new Text();
        private FloatWritable pr = new FloatWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tmp = line.split("\t");
            page.set(tmp[0]);
            pr.set(Float.parseFloat(tmp[1]));
            context.write(pr, page);
        }

    }

    public static class FloatComparator extends FloatWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }


}
