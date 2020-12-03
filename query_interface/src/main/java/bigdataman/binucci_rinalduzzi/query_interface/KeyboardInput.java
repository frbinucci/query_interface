package bigdataman.binucci_rinalduzzi.query_interface;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class KeyboardInput {
	
	public static int inputInteger()
	{
		BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
		int value = 0;
		try
		{
			value = Integer.parseInt(keyboard.readLine());
		}
		catch(IOException ioe)
		{
			System.out.println("Unexpected input!");
		}
		return value;
	}
	
	public static String inputString()
	{
		BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
		String value="";
		try 
		{
			value = keyboard.readLine();
		}
		catch(IOException ioe)
		{
			System.out.println("Unexpected input!");
		}

		return value;
	}

}
