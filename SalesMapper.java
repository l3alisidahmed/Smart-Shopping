import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Skip the header line
        if (line.startsWith("date")) return;
        
        String[] fields = line.split(",");
        
        // Extract relevant fields
        String sellerId = fields[1];
        String sellerName = fields[2];
        String productId = fields[3];
        String productName = fields[4];
        double price = Double.parseDouble(fields[5]);
        int quantityBought = Integer.parseInt(fields[6]);
        double revenue = price * quantityBought;

        // Emit: sellerId as key, and sellerName, productId, productName, revenue, quantity as value
        String valueOut = String.join("\t", sellerName, productId, productName, String.valueOf(revenue), String.valueOf(quantityBought));
        context.write(new Text(sellerId), new Text(valueOut));
    }
}
