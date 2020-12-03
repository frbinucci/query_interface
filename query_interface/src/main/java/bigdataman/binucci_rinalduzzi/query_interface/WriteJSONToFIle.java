package bigdataman.binucci_rinalduzzi.query_interface;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

public class WriteJSONToFIle {
	
	public static void writeToFile(String fileName,FindIterable<Document> document)
	{
		try 
		{
			PrintWriter pout = new PrintWriter(new FileOutputStream(fileName));
            pout.print("var time_serie = { \"data\": [");
            pout.println();
			MongoCursor<Document> it =document.iterator();
			while(it.hasNext())
			{
				Document doc = it.next();
				if(it.hasNext())
					pout.print(doc.toJson()+",");
				else
					pout.print(doc.toJson());
				pout.println();
			}
			pout.print("]}");
			pout.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
