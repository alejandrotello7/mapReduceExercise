package solutions.assignment1;

import java.io.File;
import java.util.regex.*;
import java.io.FileInputStream;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;

public class MapRedSolution1
{
    public static class MapLogs extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        private final static LongWritable one = new LongWritable(1);
        Text logAddr = new Text();
        public static final int NUM_FIELDS = 9;
        String logEntryPattern = "^(\\S+) (\\S+) (\\S+) " +  
                "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +  
                " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)"; 
        
      /*  String logEntryPattern2 = "^(\\S+) (\\S+) (\\w) " +  
                "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +  
                " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\Ss+)"; */
        
        
        Pattern p = Pattern.compile(logEntryPattern, Pattern.MULTILINE);
        //Pattern p2 = Pattern.compile(logEntryPattern2, Pattern.MULTILINE);
        
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
        	Matcher matcher = p.matcher(value.toString());
        	boolean validator = matcher.find();
        	String URL = matcher.group(6);
        	if(validator==true) {
        		String logURL = matcher.group(6);
        		boolean validURL = logURL.startsWith("http://localhost/");
        		/*if(validURL == false) {
        			logURL = "http://localhost"+logURL; 
        			}
        		else {
        			System.out.println("URL valido: "+logURL);
        		}*/
        		logAddr.set(logURL);
        		context.write(logAddr, one);
        		//System.out.println("Log URL: "+logURL);
        
        	} else{
        		 System.out.println("No entre: "+value.toString());
        	 }//else if 

        }//Map
    }//Mapper
    
    public static class ReduceLogs extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        private LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
            Context context) throws IOException, InterruptedException
        {
            int sum = 0;
        
            for (LongWritable val : values)
            sum += val.get();
            
            result.set(sum);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution1 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #1");
        
        /* your code goes in here*/
        job.setJarByClass(MapRedSolution1.class);
        job.setMapperClass(MapLogs.class);
        job.setCombinerClass(ReduceLogs.class);
        job.setReducerClass(ReduceLogs.class);
        

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        
        /* code here */
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1; 
        
        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1]+"/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();
        
        String[] validMd5Sums = {"ca11be10a928d07204702b3f950fb353", "6a70a6176249b0f16bdaeee5996f74cb", 
            "54893b270934b63a25cd0dcfd42fba64", "d947988bd6f35078131ce64db48dfad2", "3c3ded703f60e117d48c3c37e2830866"};
        System.out.println(md5);
        for (String validMd5 : validMd5Sums) 
        {
            if (validMd5.contentEquals(md5))
            {	
            	
                System.out.println("The result looks good :-)");
                System.exit(exitCode);
            }
        }
     
        System.out.println("The result does not look like what we expected :-(");
        System.exit(exitCode);
    }
}
