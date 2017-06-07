

package task4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitionzer1 extends Partitioner<Text, IntWritable>{
	
	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions){
		
		String line = key.toString();
		String val = line.substring(0,1);
		
		
		if( val.equalsIgnoreCase("a") || val.equalsIgnoreCase("b") || val.equalsIgnoreCase("c") || val.equalsIgnoreCase("d")
				|| val.equalsIgnoreCase("e") || val.equalsIgnoreCase("f") ){
			//System.out.println("reducer 1 ");
			return 0;
		}
		else if (val.equalsIgnoreCase("g") || val.equalsIgnoreCase("h") || val.equalsIgnoreCase("i") || val.equalsIgnoreCase("j")
				|| val.equalsIgnoreCase("k") || val.equalsIgnoreCase("l") ){
			return 1  ;
				
		}
		else if(val.equalsIgnoreCase("m") || val.equalsIgnoreCase("n") || val.equalsIgnoreCase("o") || val.equalsIgnoreCase("p")
				|| val.equalsIgnoreCase("q") || val.equalsIgnoreCase("r") ) {
			return 2  ;
		}
		else {
			return 3  ;
		}
			
		
	}

}
