package gr.aueb.panagiotisl.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static  void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "/");

        // instantiate a configuration
        Configuration configuration = new Configuration();

        // instantiate a job
        Job job = Job.getInstance(configuration, "Spotify Danceability");

        // Register the jar and the job's main components (Mapper / Combiner / Reducer)
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.CountMapper.class);
        job.setCombinerClass(WordCount.CountCombiner.class);
        job.setReducerClass(WordCount.CountReducer.class);

        // Mapper output types (key/value emitted by the Mapper before shuffle)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WordCount.DanceStatsWritable.class);

        // Final output types (key/value written by the Reducer to HDFS)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // Input: Spotify CSV in HDFS. Output: directory that must NOT already exist.
        FileInputFormat.addInputPath(job, new Path("/user/hdfs/spotify_input/universal_top_spotify_songs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/user/hdfs/spotify_output"));
        
        // Submit the job to YARN and exit with a proper code (0=success, 1=failure)
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
