import org.apache.commons.net.io.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fk on 17-7-15.
 */
public class WholeJob {

    private static final int times = 30;
    private static int LABEL_ITERATE_TIME = 10;

    public static void main(String[] args) throws IOException {

        String[] param = parseArgs(args[0]);

        Configuration SplitJobConf = new Configuration();
        SplitMain.addPersonName(SplitJobConf,param[0]);
        Job SplitJob = Job.getInstance(SplitJobConf,"split job");
        SplitJob.setJarByClass(SplitMain.class);
        SplitJob.setMapperClass(SplitMain.SplitMapper.class);
        SplitJob.setReducerClass(SplitMain.SplitReducer.class);
        SplitJob.setOutputKeyClass(Text.class);
        SplitJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(SplitJob,new Path(param[1]));
        FileOutputFormat.setOutputPath(SplitJob,new Path(param[2]));

        Configuration AppearCountJobConf = new Configuration();
        Job AppearCountJob = Job.getInstance(AppearCountJobConf,"appear count job");
        AppearCountJob.setJarByClass(AppearCountMain.class);
        AppearCountJob.setMapperClass(AppearCountMain.AppearCountMapper.class);
        AppearCountJob.setCombinerClass(AppearCountMain.AppearCountReducer.class);
        AppearCountJob.setReducerClass(AppearCountMain.AppearCountReducer.class);
        AppearCountJob.setOutputKeyClass(Text.class);
        AppearCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(AppearCountJob, new Path(param[2]));
        FileOutputFormat.setOutputPath(AppearCountJob, new Path(param[3]));

        Configuration NormalizeJobConf = new Configuration();
        Job NormalizeJob = Job.getInstance(NormalizeJobConf, "Graph");
        NormalizeJob.setJarByClass(Graph.class);
        NormalizeJob.setMapperClass(Graph.MyMapper.class);
        NormalizeJob.setMapOutputKeyClass(Text.class);
        NormalizeJob.setMapOutputValueClass(IntWritable.class);
        NormalizeJob.setCombinerClass(Graph.MyCombiner.class);
        NormalizeJob.setPartitionerClass(Graph.MyPartitioner.class);
        NormalizeJob.setReducerClass(Graph.MyReducer.class);
        NormalizeJob.setOutputKeyClass(Text.class);
        NormalizeJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(NormalizeJob, new Path(param[3]));
        FileOutputFormat.setOutputPath(NormalizeJob, new Path(param[4]));

        Configuration GraphBuilderJobConf = new Configuration();
        Job GraphBuilderJob = Job.getInstance(GraphBuilderJobConf, "GraphBuilder");
        GraphBuilderJob.setJarByClass(GraphBuilder.class);
        GraphBuilderJob.setMapperClass(GraphBuilder.MyMapper.class);
        GraphBuilderJob.setOutputKeyClass(Text.class);
        GraphBuilderJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(GraphBuilderJob, new Path(param[4]));
        FileOutputFormat.setOutputPath(GraphBuilderJob, new Path(param[5] + "/Data0"));

        Configuration[] PageRankConf = new Configuration[times];
        Job[] PageRankJob = new Job[times];
        for(int i = 0;i < times;i++) {
            PageRankConf[i] = new Configuration();
            PageRankJob[i] = Job.getInstance(PageRankConf[i], "Pagerank_" + i);
            PageRankJob[i].setJarByClass(PageRank.class);
            PageRankJob[i].setMapperClass(PageRank.MyMapper.class);
            PageRankJob[i].setMapOutputKeyClass(Text.class);
            PageRankJob[i].setMapOutputValueClass(Text.class);
            PageRankJob[i].setReducerClass(PageRank.MyReducer.class);
            PageRankJob[i].setOutputKeyClass(Text.class);
            PageRankJob[i].setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(PageRankJob[i], new Path(param[5] + "/Data" + i));
            FileOutputFormat.setOutputPath(PageRankJob[i], new Path(param[5] + "/Data" + (i + 1)));
        }

        Configuration PageRankSortConf = new Configuration();
        Job PageRankSortJob = Job.getInstance(PageRankSortConf, "PageRankSort");
        PageRankSortJob.setJarByClass(PageRankSort.class);
        PageRankSortJob.setOutputKeyClass(FloatWritable.class);
        PageRankSortJob.setOutputValueClass(Text.class);
        PageRankSortJob.setSortComparatorClass(PageRankSort.FloatComparator.class);
        PageRankSortJob.setMapperClass(PageRankSort.MyMapper.class);
        FileInputFormat.addInputPath(PageRankSortJob, new Path(param[5] + "/Data" + times));
        FileOutputFormat.setOutputPath(PageRankSortJob, new Path(param[5] + "/FinalRank"));

        Configuration StatDegreeJobConf = new Configuration();
        Job StatDegreeJob = Job.getInstance(StatDegreeJobConf,"stat degree");
        StatDegreeJob.setJarByClass(Stat_Degree.class);
        StatDegreeJob.setMapperClass(Stat_Degree.Degree_Mapper.class);
        StatDegreeJob.setReducerClass(Stat_Degree.Degree_Reducer.class);
        StatDegreeJob.setMapOutputKeyClass(IntWritable.class);
        StatDegreeJob.setMapOutputValueClass(Text.class);
        StatDegreeJob.setSortComparatorClass(Stat_Degree.IntComparator.class);
        StatDegreeJob.setOutputKeyClass(Text.class);
        StatDegreeJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(StatDegreeJob, new Path(param[4]));
        FileOutputFormat.setOutputPath(StatDegreeJob, new Path(param[6]+"/degree"));

        Configuration labelInitJobConf = new Configuration();
        labelInitJobConf.set("peoplename_path" , param[0]);//设置全局共享参数
        Job labelInitJob = Job.getInstance(labelInitJobConf, "init labels");
        labelInitJob.setJarByClass(Init_Label.class);
        labelInitJob.setMapperClass(Init_Label.TokenizerMapper.class);
        labelInitJob.setReducerClass(Init_Label.IntSumReducer.class);
        labelInitJob.setOutputKeyClass(Text.class);
        labelInitJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(labelInitJob, new Path(param[4]));
        FileOutputFormat.setOutputPath(labelInitJob, new Path(param[6] + "/label"));

        Configuration preproLabelJobConf = new Configuration();
        preproLabelJobConf.set("peopleDegree_path" , param[6]+"/degree");//设置全局共享参数
        preproLabelJobConf.set("peoplename_label_path",param[6]+"/label");
        Job preproLabelJob = Job.getInstance(preproLabelJobConf, "Preprop_Label");
        preproLabelJob.setJarByClass(Preprop_Label.class);
        preproLabelJob.setMapperClass(Preprop_Label.Preprop_Mapper.class);
        preproLabelJob.setReducerClass(Preprop_Label.Preprop_Reducer.class);
        preproLabelJob.setOutputKeyClass(Text.class);
        preproLabelJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(preproLabelJob, new Path(param[4]));
        FileOutputFormat.setOutputPath(preproLabelJob, new Path(param[6]+"/label"+String.valueOf(0)));

        Configuration[] labelProJobConf = new Configuration[LABEL_ITERATE_TIME];
        Job[] labelProJob = new Job[LABEL_ITERATE_TIME];
        for(int i=0;i<LABEL_ITERATE_TIME;++i)
        {
            labelProJobConf[i] = new Configuration();
            labelProJobConf[i].set("peoplename_label_path",param[6]+"/label"+String.valueOf(i));
            labelProJob[i] = Job.getInstance(labelProJobConf[i], "prop labels_"+i);
            labelProJob[i].setJarByClass(Prop_label.class);
            labelProJob[i].setMapperClass(Prop_label.Prop_Mapper.class);
            labelProJob[i].setReducerClass(Prop_label.Prop_Reducer.class);
            labelProJob[i].setOutputKeyClass(Text.class);
            labelProJob[i].setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(labelProJob[i], new Path(param[4]));
            FileOutputFormat.setOutputPath(labelProJob[i], new Path(param[6]+"/label"+String.valueOf(i+1)));
        }

        Configuration labelOutputJobConf = new Configuration();
        Job labelOutputJob = Job.getInstance(labelOutputJobConf,"label output");
        labelOutputJob.setJarByClass(LabelOutputMain.class);
        labelOutputJob.setMapperClass(LabelOutputMain.LabelOutputMapper.class);
        labelOutputJob.setCombinerClass(LabelOutputMain.LabelOutputReducer.class);
        labelOutputJob.setReducerClass(LabelOutputMain.LabelOutputReducer.class);
        labelOutputJob.setOutputKeyClass(IntWritable.class);
        labelOutputJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(labelOutputJob,new Path(param[6]+"/label"+String.valueOf(LABEL_ITERATE_TIME)));
        FileOutputFormat.setOutputPath(labelOutputJob,new Path(param[7]));

        ControlledJob controlledSplitJob = new ControlledJob(SplitJobConf);
        ControlledJob controlledAppearCountJob = new ControlledJob(AppearCountJobConf);
        ControlledJob controlledNormalizeJob = new ControlledJob(NormalizeJobConf);
        ControlledJob controlledGraphBuilderJob = new ControlledJob(GraphBuilderJobConf);
        ControlledJob[] controlledPageRankJob = new ControlledJob[times];
        for(int i = 0;i < times;i++) {
            controlledPageRankJob[i] = new ControlledJob(PageRankConf[i]);
        }
        ControlledJob controlledPageRankSortJob = new ControlledJob(PageRankSortConf);
        ControlledJob controlledStatDegreeJob = new ControlledJob(StatDegreeJobConf);
        ControlledJob controlledLabelInitJob = new ControlledJob(labelInitJobConf);
        ControlledJob controlledPreproLabelJob = new ControlledJob(preproLabelJobConf);
        ControlledJob[] controlledLabelProJob = new ControlledJob[LABEL_ITERATE_TIME];
        for(int i=0;i<LABEL_ITERATE_TIME;++i)
        {
            controlledLabelProJob[i] = new ControlledJob(labelProJobConf[i]);
        }
        ControlledJob controlledLabelOutputJob = new ControlledJob(labelOutputJobConf);

        controlledSplitJob.setJob(SplitJob);
        controlledAppearCountJob.setJob(AppearCountJob);
        controlledNormalizeJob.setJob(NormalizeJob);
        controlledGraphBuilderJob.setJob(GraphBuilderJob);
        for(int i = 0;i < times;i++) {
            controlledPageRankJob[i].setJob(PageRankJob[i]);
        }
        controlledPageRankSortJob.setJob(PageRankSortJob);
        controlledStatDegreeJob.setJob(StatDegreeJob);
        controlledLabelInitJob.setJob(labelInitJob);
        controlledPreproLabelJob.setJob(preproLabelJob);
        for(int i=0;i<LABEL_ITERATE_TIME;++i)
        {
            controlledLabelProJob[i].setJob(labelProJob[i]);
        }
        controlledLabelOutputJob.setJob(labelOutputJob);

        controlledAppearCountJob.addDependingJob(controlledSplitJob);
        controlledNormalizeJob.addDependingJob(controlledAppearCountJob);
        controlledGraphBuilderJob.addDependingJob(controlledNormalizeJob);
        for(int i = 0;i < times;i++) {
            if(i == 0) {
                controlledPageRankJob[i].addDependingJob(controlledGraphBuilderJob);
            }
            else {
                controlledPageRankJob[i].addDependingJob(controlledPageRankJob[i-1]);
            }
        }
        controlledPageRankSortJob.addDependingJob(controlledPageRankJob[times-1]);
        controlledStatDegreeJob.addDependingJob(controlledNormalizeJob);
        controlledLabelInitJob.addDependingJob(controlledStatDegreeJob);
        controlledPreproLabelJob.addDependingJob(controlledLabelInitJob);
        for(int i=0;i<LABEL_ITERATE_TIME;++i)
        {
            if(i==0)
            {
                controlledLabelProJob[i].addDependingJob(controlledPreproLabelJob);
            }
            else
            {
                controlledLabelProJob[i].addDependingJob(controlledLabelProJob[i-1]);
            }
        }
        controlledLabelOutputJob.addDependingJob(controlledLabelProJob[LABEL_ITERATE_TIME-1]);

        JobControl jc = new JobControl("task2");
        jc.addJob(controlledSplitJob);
        jc.addJob(controlledAppearCountJob);
        jc.addJob(controlledNormalizeJob);
        jc.addJob(controlledGraphBuilderJob);
        for(int i = 0;i < times;i++) {
            jc.addJob(controlledPageRankJob[i]);
        }
        jc.addJob(controlledPageRankSortJob);
        jc.addJob(controlledStatDegreeJob);
        jc.addJob(controlledLabelInitJob);
        jc.addJob(controlledPreproLabelJob);
        for(int i=0;i<LABEL_ITERATE_TIME;++i)
        {
            jc.addJob(controlledLabelProJob[i]);
        }
        jc.addJob(controlledLabelOutputJob);
        Thread thread = new Thread(jc);
        thread.start();
        while(true)
        {
            if(jc.allFinished())
            {
                jc.stop();
                for(int i=0;i<times;++i)
                {
                    Utility.deleteDir(param[5]+"/Data"+String.valueOf(i));
                }
                for(int i=0;i<LABEL_ITERATE_TIME;++i)
                {
                    Utility.deleteDir(param[6]+"/label"+String.valueOf(i));
                }
                System.exit(0);
            }
        }
    }

    private static String[] parseArgs(String str)
    {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(str))));
            String line = reader.readLine();
            return line.split(" ");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
