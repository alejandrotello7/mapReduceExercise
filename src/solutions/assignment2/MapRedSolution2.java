package solutions.assignment2;

import java.io.File;
import java.util.regex.*;
import java.io.FileInputStream;
import java.util.Scanner;
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

public class MapRedSolution2
{
	 public static class MapTaxi extends Mapper<LongWritable, Text, Text, LongWritable>
	    {
	        private final static LongWritable one = new LongWritable(1);
	        Text timeSlot = new Text();
	        String[] valoresTemporal = new String[20]; 
	        
	        
	        @Override
	        protected void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException
	        {
	        	try {
	        		
	        	
	        	int i = 0;
	        	Scanner scanner = new Scanner(value.toString());
	        	scanner.useDelimiter(",");
	            while (scanner.hasNext()) 
	            {
	                valoresTemporal[i]= scanner.next();
	                i++;
	            }
	            scanner.close();
	           // System.out.println("Vendor ID: "+valoresTemporal[0]);
	            //System.out.println("Pick-up time: "+valoresTemporal[1]);
	            //System.out.println("Drop Off time: "+valoresTemporal[2]);
	            //System.out.println("----------------------");
	            String[] timeSplitterPick = valoresTemporal[1].split(" ");
	            String[] timeSplitterDrop = valoresTemporal[2].split(" ");
	            if(timeSplitterPick.length==1) {
	            	System.out.println(value.toString());
	            }
	            //System.out.println(value.toString());
	            String[] timeSplitterPick2 = timeSplitterPick[1].split(":");
	            String[] timeSplitterDrop2 = timeSplitterDrop[1].split(":");
	            
	            //System.out.println(timeSplitterPick[1]);
	            //System.out.println(timeSplitterDrop[1]);
	            /*System.out.println(timeSplitterPick2[0]);
	            System.out.println(timeSplitterDrop2[0]);
	            System.out.println("----------------------");*/  
	            
	            int pickTime = Integer.parseInt(timeSplitterPick2[0]);
	            int dropTime = Integer.parseInt(timeSplitterDrop2[0]);
	            String timeSlotString;
	            int holder=0;

	  
	            if(pickTime == dropTime) {
	            	if(pickTime >= 0 && pickTime <12) {
	            		if(pickTime==0) {
		            		timeSlotString ="12am";
		            		timeSlot.set(timeSlotString);
		            		context.write(timeSlot, one);
	            		}else {
		            		timeSlotString =pickTime+"am";
		            		timeSlot.set(timeSlotString);
		            		context.write(timeSlot, one);
	            		}
	            	}else if(pickTime > 12 && pickTime != 12) {
	            		timeSlotString =(pickTime-12)+"pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	} else if(pickTime == 12) {
	            		timeSlotString ="12pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}
	            	
	            } else if(pickTime < dropTime) {
	            	//PickTime
	            	if(pickTime >=0 && pickTime <12) {
	            		if(pickTime==0) {
		            		timeSlotString ="12am";
		            		timeSlot.set(timeSlotString);
		            		context.write(timeSlot, one);
	            		}else {
		            		timeSlotString =pickTime+"am";
		            		timeSlot.set(timeSlotString);
		            		context.write(timeSlot, one);
	            		}

	            	}else if (pickTime > 12 && pickTime != 12) {
	            		timeSlotString =(pickTime-12)+"pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}else if (pickTime == 12) {
	            		timeSlotString ="12pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}
	            	
	            	
	            	
	            	/*//DropTime
	            	if(dropTime >=0 && dropTime <12) {
	            		timeSlotString =dropTime+"am";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}else if (dropTime > 12 && dropTime != 12) {
	            		timeSlotString =(dropTime-12)+"pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}else if (dropTime == 12) {
	            		timeSlotString ="12pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}*/
	            } else if (pickTime > dropTime) {
	            	//PickTime
	            	if(pickTime >=0 && pickTime <12) {
	            		if(pickTime==0) {
		            		timeSlotString ="12am";
		            		timeSlot.set(timeSlotString);
		            		context.write(timeSlot, one);
	            		}else {
		            		timeSlotString =pickTime+"am";
		            		timeSlot.set(timeSlotString);
		            		context.write(timeSlot, one);
	            		}

	            	}else if (pickTime > 12 && pickTime != 12) {
	            		timeSlotString =(pickTime-12)+"pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}else if (pickTime == 12) {
	            		timeSlotString ="12pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}
	            	
	            	/*//DropTime
	            	if(dropTime >=0 && dropTime <12) {
	            		timeSlotString =dropTime+"am";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}else if (dropTime > 12 && dropTime != 12) {
	            		timeSlotString =(dropTime-12)+"pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}else if (dropTime == 12) {
	            		timeSlotString ="12pm";
	            		timeSlot.set(timeSlotString);
	            		context.write(timeSlot, one);
	            	}*/
	            }
	            
	            
	          /*  0-12:59:59
	            
	            0:15:00 - 0:20:00
	            0:15:00 - 1:15:00
	            10:20:00 - 11:20:00
	            12:40:00 - 13:30:00
	            11:35:00 - 11:40:00*/
	           
	            
	           
	        	//System.out.println(value.toString());
	        	//timeSlot.set("test");
	        	//context.write(timeSlot, one);
	        	} //try
	        	catch (IndexOutOfBoundsException e) {
	        		System.out.println(value.toString());
	        	}
	        }//Map Class
	    }//Mapper Class
	 
	    public static class ReduceTaxi extends Reducer<Text, LongWritable, Text, LongWritable>
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
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #2");
        
        /* code goes in here*/
        
        job.setJarByClass(MapRedSolution2.class);
        job.setMapperClass(MapTaxi.class);
        job.setCombinerClass(ReduceTaxi.class);
        job.setReducerClass(ReduceTaxi.class);
        

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        
        /* Placeholder for code */
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1; 

        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1]+"/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();
        
        String[] validMd5Sums = {"03357cb042c12da46dd5f0217509adc8", "ad6697014eba5670f6fc79fbac73cf83", "07f6514a2f48cff8e12fdbc533bc0fe5", 
            "e3c247d186e3f7d7ba5bab626a8474d7", "fce860313d4924130b626806fa9a3826", "cc56d08d719a1401ad2731898c6b82dd", 
            "6cd1ad65c5fd8e54ed83ea59320731e9", "59737bd718c9f38be5354304f5a36466", "7d35ce45afd621e46840627a79f87dac"};
        
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
