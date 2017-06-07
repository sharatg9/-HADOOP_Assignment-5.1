

package task5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key,Text value, Context context)throws IOException , InterruptedException{
		
		String line = value.toString().replace("|", ",");                         //convert the value into string 			
		String[] arr= line.split(",");						// split operation carried out 
		String company = arr[0];                                // putting company name into new string 
		
		context.write(new Text(company), new IntWritable(1));   //  ex : Apple,1 
		
		
	}

}
