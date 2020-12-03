package bigdataman.binucci_rinalduzzi.query_interface;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.model.Projections;
import com.mongodb.BasicDBList;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.connection.ClusterSettings.Builder;

/**
 * Hello world!
 *
 */
public class QueryInterface {
	public static void main( String[] args )
    {
    	MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
    	
    	 MongoDatabase database = mongoClient.getDatabase("test");
    	 MongoCollection<Document> collection = database.getCollection("years");
    	 
    	 int choiche=1;
    	 while(choiche!=0)
    	 {
	    	 System.out.println("Which collection do you want to query?");
	    	 System.out.println("1 - for years");
	    	 System.out.println("2 - for manufacturers");
	    	 System.out.println("3 - for displacements");
	    	 System.out.println("4 - for vehicle categories");
	    	 System.out.println("5 - generate timeseries to render charts");
	    	 System.out.println("0 - exit");

	    	 choiche = KeyboardInput.inputInteger();
	    	 int yearChoiche;
	    	 
	    	 switch(choiche)
	    	 {
		    	 case 1:
			    		 System.out.print("Insert a year: ");
			    		 String year = KeyboardInput.inputString();
		    			 Document yearDoc = collection.find(eq("_id", year)).first();
		    			 if(yearDoc!=null)
		    			 {
				    		 do
				    		 {
					    		 System.out.println("Which information do you want to know about "+year);
					    		 System.out.println("1 - Max consumption model with consumption");
					    		 System.out.println("2 - Number of models by cylinder number");
					    		 System.out.println("3 - Mean emissions");
					    		 System.out.println("0 - Go back");
					    		 yearChoiche = KeyboardInput.inputInteger();
					    		 switch(yearChoiche)
					    		 {
					    		 	case 1:
					    		 		String model = yearDoc.get("max_cons_model").toString();
					    		 		String max_cons = yearDoc.get("max_cons").toString();
					    		 		System.out.println("The model is: "+model);
					    		 		System.out.println("Max consumption is: "+max_cons);
					    		 		System.out.println();
					    		 		break;
					    		 	case 2:
					    		 		List<Document> obj = (List) yearDoc.get("number_models_by_cyl_number");
					    		 		
					    		 		System.out.println(obj.size());
					    		 		for(Document doc : obj)
					    		 		{
						    		 		JSONObject json_array = new JSONObject(doc.toJson());
	
						    		 		Iterator<?> keys = json_array.keys();
						    		 		System.out.println();
						    		 		while( keys.hasNext() ) {
						    		 		    String cyl_number = (String) keys.next();
						    		 		    System.out.println("Number of cylinders: " + cyl_number);
						    		 		    System.out.println("Number of cars: " + json_array.get(cyl_number));
						    		 		}
	
					    		 		}
					    		 		System.out.println();
	
					    		 		break;
					    		 	case 3:
					    		 		String meanEmissions = yearDoc.get("mean_emissions").toString();
					    		 		System.out.println("Mean emissions for "+year+ "are: "+meanEmissions);
					    		 		System.out.println();
					    		 		break;
					    		 	default:
					    		 		break;
					    		 }
					    		 
				    		 }while(yearChoiche!=0);
		    			 }
		    			 else
		    			 {
		    				 System.out.println("There are no data associated to "+year);
		    			 }
			    		break;
			    	default:
			    		break;
			    case 5:
			    	System.out.println("Which kind of timeseries do you want to generate?");
			    	System.out.println("1 - for max consumption");
			    	System.out.println("2 - for mean emissions");
			    	int timeSeriesChoiche = KeyboardInput.inputInteger();
			    	System.out.print("Insert year start: ");
			    	String yearStart = KeyboardInput.inputString();
			    	System.out.print("Insert year stop: ");
			    	String yearStop = KeyboardInput.inputString();
			    	FindIterable<Document> timeSeriesDoc;
			    	if(timeSeriesChoiche==1)
			    	{
			    		timeSeriesDoc = collection.find(and(gte("_id", yearStart), lte("_id", yearStop))).projection(Projections.include("_id", "max_cons"));
			    		WriteJSONToFIle.writeToFile("max_emissions"+yearStart+"_"+yearStop+".js",timeSeriesDoc);
			    	}
			    	else
			    	{
			    		timeSeriesDoc = collection.find(and(gte("_id", yearStart), lte("_id", yearStop))).projection(Projections.include("_id", "mean_emissions"));
			    		WriteJSONToFIle.writeToFile("mean_emissions"+yearStart+"_"+yearStop+".js",timeSeriesDoc);
			    	}
			    		
			    	break;
			    		
	    	 }
	    	 
	    	 
    	 } 
    	 
    }
}
