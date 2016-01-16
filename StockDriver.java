import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;	
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
public class StockDriver 
{
	public static void main(String[] args) throws Exception 
	{
		if (args.length!=2) 
		{
			System.out.printf("Usage: StockDriver <input dir> <output dir> \n");
			System.exit(-1);
		}
		System.out.println("Select what you want to do:\n1. For a given ticker and date range, count the number of days when the stock closed higher than the previous day and the difference with previous day's price was x% or higher. \n2. For a given ticker and date range, count the number of days when the stock closed lower than the previous day and the difference with previous day's price was x% or lower. \n3. For a given pair of tickers (ticker1 and ticker2) and date range, count the number of days when ticker1 closed higher than ticker2.\n4. For a given ticker and date range, count the number of days when the volume of the stock closed higher than the previous day and the difference with previous day's volume was x% or higher.\n5. For a given ticker and date range, count the number of days when the volume of the stock closed lower than the previous day and the difference with previous day's volume was x% or lower.\n6. For a given pair of tickers (ticker1 and ticker2) and date range, count the number of days when the volume of ticker1 closed higher than the volume of ticker2.");
		Scanner sc=new Scanner(System.in);
		Integer choice = sc.nextInt();
		Configuration conf=new Configuration();
		String startDate, endDate, ticker, percentInc, ticker1, ticker2;
		if ( choice == 1 || choice == 2 || choice == 4 || choice== 5 || choice!=3 || choice!=6);
		{
			System.out.println("Enter ticker: ");
			ticker = sc.next();
			System.out.println("Enter start date (mm/dd/yyyy): ");
			startDate = sc.next();
			System.out.println("Enter end date (mm/dd/yyyy): ");
			endDate = sc.next();
			System.out.println("Enter percent : ");
			percentInc = sc.next();

			conf.set("peri", percentInc);
			conf.set("startDate", startDate);
			conf.set("endDate", endDate);
			conf.set("ticker", ticker);
			conf.set("choice", choice.toString());
		}
		if(choice == 3 || choice == 6)
		{
			System.out.println("Ticker 1: ");
			ticker1 = sc.next();
			System.out.println("Ticker 2: ");
			ticker2 = sc.next();
			System.out.println("Enter start date (mm/dd/yyyy): ");
			startDate = sc.next();
			System.out.println("Enter end date (mm/dd/yyyy): ");
			endDate = sc.next();
			//System.out.println("Enter percent : ");
			//percentInc = sc.next();

			//conf.set("peri", percentInc);
			conf.set("startDate", startDate);
			conf.set("endDate", endDate);
			conf.set("ticker1", ticker1);
			conf.set("ticker2", ticker2);
			conf.set("choice", choice.toString());
		}

		Job job = new Job(conf);

		job.setJarByClass(StockDriver.class);
		job.setJobName("Stock Driver");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(StockMapper.class);
		job.setReducerClass(StockReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}