import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String sellerName = "";
        double totalRevenue = 0.0;
        String maxProductId = "";
        String maxProductName = "";
        int maxQuantity = 0;

        // Iterate through values for a seller
        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            sellerName = fields[0];
            String productId = fields[1];
            String productName = fields[2];
            double revenue = Double.parseDouble(fields[3]);
            int quantityBought = Integer.parseInt(fields[4]);

            // Update total revenue
            totalRevenue += revenue;

            // Check for the product with the highest quantity
            if (quantityBought > maxQuantity) {
                maxQuantity = quantityBought;
                maxProductId = productId;
                maxProductName = productName;
            }
        }

        // Emit the seller ID, total revenue, and product with max quantity
        String result = String.format("%s\t%.2f\t%s\t%s\t%d", sellerName, totalRevenue, maxProductId, maxProductName, maxQuantity);
        context.write(key, new Text(result));
    }
}
