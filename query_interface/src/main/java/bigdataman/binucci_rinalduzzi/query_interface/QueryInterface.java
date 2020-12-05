package bigdataman.binucci_rinalduzzi.query_interface;

import java.util.Arrays;
import java.util.Comparator;
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
    	 MongoCollection<Document> years = database.getCollection("years");
    	 MongoCollection<Document> manufacturers = database.getCollection("manufacturers");
    	 MongoCollection<Document> displacement = database.getCollection("displacement");
    	 MongoCollection<Document> vehicle_category = database.getCollection("vehicle_category");
    	 
    	 int choiche=1;
    	 while(choiche!=0)
    	 {
    		 System.out.println();
	    	 System.out.println("Which collection do you want to query?");
	    	 System.out.println("1 - for years");
	    	 System.out.println("2 - for manufacturers");
	    	 System.out.println("3 - for displacements");
	    	 System.out.println("4 - for vehicle categories");
	    	 System.out.println("5 - generate timeseries to render charts");
	    	 System.out.println("0 - exit");

	    	 choiche = KeyboardInput.inputInteger();
	    	 int yearChoiche;
	    	 int manChoiche;
	    	 
	    	 switch(choiche)
	    	 {
		    	 case 1:
			    		 System.out.print("Insert a year: ");
			    		 String year = KeyboardInput.inputString();
		    			 Document yearDoc = years.find(eq("_id", year)).first();
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
					    		 		System.out.println("Mean emissions for "+year+ " are: "+meanEmissions);
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
			    case 2:
			    	System.out.print("Insert a manufacturer (e.g. \"Audi\"): ");
		    		String manufacturer = KeyboardInput.inputString();
	    			Document manDoc = manufacturers.find(eq("_id", manufacturer)).first();
	    			if(manDoc!=null) {
	    				do {
	    					System.out.println("Which information do you want to know about "+manufacturer);
				    		System.out.println("1 - Min consumption model with consumption");
				    		System.out.println("2 - Max emissions by year");
				    		System.out.println("3 - Mean emissions");
				    		System.out.println("0 - Go back");
				    		manChoiche = KeyboardInput.inputInteger();
				    		switch(manChoiche) {
					    		case 1:
				    		 		String model = manDoc.get("min_cons_model").toString();
				    		 		String min_year = manDoc.get("min_cons_year").toString();
				    		 		String min_cons = manDoc.get("min_cons").toString();
				    		 		System.out.println("The model is: "+model+", from year "+min_year);
				    		 		System.out.println("Min consumption is: "+min_cons);
				    		 		System.out.println();
				    		 		break;
				    		 	case 2:
				    		 		List<Document> obj = (List) manDoc.get("max_emissions_by_year");
				    		 		//System.out.println(obj.size());
				    		 		for(Document doc : obj){
				    		 			String y = doc.getString("year");
				    		 			String m = doc.getString("model");
				    		 			String e = doc.getString("emission");
				    		 			System.out.println("YEAR: "+y+", MODEL: "+m+", EMISSION: "+e);				    		 			
				    		 		}
				    		 		System.out.println();			
				    		 		break;
				    		 	case 3:
				    		 		String meanEmissions = manDoc.get("mean_emissions").toString();
				    		 		System.out.println("Mean emissions for "+manufacturer+ " are: "+meanEmissions);
				    		 		System.out.println();
				    		 		break;
				    		 	default:
				    		 		break;
				    		}
	    				}while(manChoiche!=0);

	    				
	    			}else{
	    				System.out.println("There are no data associated to "+manufacturer);
	    			}
	    			break;
			    case 3:
			    	String disp="";
			    	do {
				    	System.out.print("Insert a displacement (e.g. \"2.0\") and I will show you the mean consumption"
				    			+ " of an engine of that displacement (type \"exit\" to exit): ");
				    	disp = KeyboardInput.inputString();
		    			Document dispDoc = displacement.find(eq("_id", disp)).first();
		    			if(dispDoc!=null) {
		    				String mean_cons = dispDoc.get("mean_consumption").toString();
		    		 		System.out.println("Mean consumption is: "+mean_cons);
		    		 		System.out.println();
		    			}else {
		    				System.out.println("There are no data associated to "+disp);
		    			}
			    	}while(!disp.equals("exit"));
			    	break;
			    case 4:
			    	String cat="";
			    	do {
				    	System.out.print("Insert a vehicle category (e.g. \"Midsize Cars\") and I will show you the mean consumption"
				    			+ " of that category (insert 0 to exit): ");
				    	cat = KeyboardInput.inputString();
		    			Document catDoc = vehicle_category.find(eq("_id", cat)).first();
		    			if(catDoc!=null) {
		    				String mean_cons = catDoc.get("mean_consumption").toString();
		    		 		System.out.println("Mean consumption is: "+mean_cons);
		    		 		System.out.println();
		    			}else {
		    				System.out.println("There are no data associated to "+cat);
		    			}
			    	}while(!cat.equals("exit"));
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
			    		timeSeriesDoc = years.find(and(gte("_id", yearStart), lte("_id", yearStop))).projection(Projections.include("_id", "max_cons"));
			    		WriteJSONToFIle.writeToFile("max_emissions"+yearStart+"_"+yearStop+".js",timeSeriesDoc);
			    	}
			    	else
			    	{
			    		timeSeriesDoc = years.find(and(gte("_id", yearStart), lte("_id", yearStop))).projection(Projections.include("_id", "mean_emissions"));
			    		WriteJSONToFIle.writeToFile("mean_emissions"+yearStart+"_"+yearStop+".js",timeSeriesDoc);
			    	}
			    		
			    	break;
			    		
	    	 }
	    	 
	    	 
    	 } 
    	 
    }
}
