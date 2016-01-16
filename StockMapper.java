import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class StockMapper extends Mapper <LongWritable, Text, Text, Text>
{
	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException
	{
		String line[]=(value.toString()).split("[,]");
		String sendThis=line[1]+","+line[2]+","+line[3];
		context.write(new Text(line[0]),new Text(sendThis));
	}
}	