import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockReducer extends Reducer<Text,Text,Text,Text>
{
	private double[] ticker1close=new double[10000];
	private double[] ticker2close=new double[10000];
	private double[] ticker1volume=new double[10000];
	private double[] ticker2volume=new double[10000];
	static int i=0,j=0,a=0,b=0;
	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
	{
		double pastValue=0.0,per=0.0,pastVolume=0.0;
		int count=0,maxdays=0,countv=0,maxdaysv=0;
		Configuration conf=context.getConfiguration();
		int choice=Integer.parseInt(conf.get("choice"));
		String startDate=null,endDate=null,ticker1=null,ticker2=null,ticker=null;
		if(choice==1||choice==2||choice==4||choice==5)
		{	
			per =Double.parseDouble(conf.get("peri"));
			startDate=conf.get("startDate");
			endDate=conf.get("endDate");
			ticker=conf.get("ticker");
		}
		if(choice==3 || choice==6)
		{
			ticker1=conf.get("ticker1");
			ticker2=conf.get("ticker2");
			startDate=conf.get("startDate");
			endDate=conf.get("endDate");
		}
		boolean flag=false;
		for(Text value :values)
		{
			String valueInString=value.toString();
			String valueArray[]=valueInString.split("[,]");
			String dateInString=valueArray[0];
			double closev=Double.parseDouble(valueArray[1]);
			double close=Double.parseDouble(valueArray[2]);
			SimpleDateFormat formatter=new SimpleDateFormat("MM/dd/yyyy");
			Date date=new Date();
			Date startdate=new Date();
			Date enddate=new Date();
			try
			{
				date=formatter.parse(dateInString);
				startdate=formatter.parse(startDate);
				enddate=formatter.parse(endDate);
			}
			catch(ParseException e)
			{
				e.printStackTrace();
			}
			flag=false;
			if(date.after(startdate)||date.equals(startdate))
			{
				flag=true;
			}
			if(date.after(enddate))
			{
				flag=false;
			}
			switch(choice)
			{
				case 1:
						if(flag)
						{
							maxdays++;
							if(close>=(((100.0+per)*pastValue)/100.0))
								count++;
						}
						break;
				case 2:
						if(flag)
						{
							maxdays++;
							if(pastValue<=((per*pastValue/100)+close))
								count++;
						}
						break;
				case 3:
						if(flag==true && (key.toString()).equals(ticker1))
						{
							ticker1close[i]=close;
							i++;
						}
						if(flag==true && (key.toString()).equals(ticker2))
						{
							ticker2close[j]=close;
							j++;
						}
						break;
				case 4:
						if(flag)
						{
							maxdaysv++;
							if(closev>=(((100+per)*pastVolume)/100))
								countv++;
						}
						break;
				case 5:
						if(flag)
						{
							maxdaysv++;
							if(pastVolume<=((per*pastVolume/100)+closev))
								countv++;
						}
						break;
				case 6:
						if(flag==true && (key.toString()).equals(ticker1))
						{
							ticker1volume[a]=closev;
							a++;
						}
						if(flag==true && (key.toString()).equals(ticker2))
						{
							ticker2volume[b]=closev;
							b++;
						}
						break;
			}
			pastValue=close;
			pastVolume=closev;
		}
		count=count-1;
		countv=countv-1;	
		int case3count1=0;
		int case3count2=0;
		int case6count1=0;
		int case6count2=0;
		for(int k=0,l=0;k<=i-1 && l<=j-1;k++,l++)
			if(ticker1close[k]>ticker2close[l])
				case3count1++;
			else
				case3count2++;	
		for(int c=0,d=0;c<=a-1 && d<=b-1;c++,d++)
			if(ticker1volume[c]>ticker2volume[d])
				case6count1++;
			else
				case6count2++;
		String answer=new String();
		switch(choice)
		{
			case 1:
				answer="Total no of days "+key.toString()+" closed "+per+"% or HIGHER of previous days = "+count+"/"+maxdays;
				break;
			case 2:
				answer="Total no of days "+key.toString()+" closed "+per+"% or LOWER of previous days = "+count+"/"+maxdays;
				break;
			case 3:
				if((key.toString()).equals(ticker1))
					answer="closed higher on "+case3count1+" days.";
				if((key.toString()).equals(ticker2))
					answer="closed higher on "+case3count2+" days.";
				break;
			case 4:
				answer="Total no of days when volume of "+key.toString()+" closed "+per+"% or HIGHER of its previous days volume = "+countv+"/"+maxdaysv;
				break;
			case 5:
				answer="Total no of days when volume of "+key.toString()+" closed "+per+"% or LOWER of its previous days volume = "+countv+"/"+maxdaysv;
				break;
			case 6:
				if((key.toString()).equals(ticker1))
					answer="had higher volume on "+case6count1+" days.";
				if((key.toString()).equals(ticker2))
					answer="had higher volume on "+case6count2+" days.";
				break;
		}
		switch(choice)
		{
			case 1:
				if((key.toString()).equals(ticker))
					context.write(key,new Text(answer));
				break;
			case 2:
				if((key.toString()).equals(ticker))
					context.write(key,new Text(answer));
				break;
			case 3:
				if((key.toString()).equals(ticker1) || (key.toString()).equals(ticker2))
					context.write(key,new Text(answer));
				break;
			case 4:
				if((key.toString()).equals(ticker))
					context.write(key,new Text(answer));
				break;
			case 5:
				if((key.toString()).equals(ticker))
					context.write(key,new Text(answer));
				break;
			case 6:
				if((key.toString()).equals(ticker1) || (key.toString()).equals(ticker2))
					context.write(key,new Text(answer));
				break;
		}
	}
}