import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.ResultSet;

import com.mysql.jdbc.ResultSetMetaData;

public class Hw3 {

	
	// initialize instances  
	// for connection object for db connection	
	public static Connection conn;
	public static Statement s;
	// initialize instances
	// driver for db and info for db connect
	private static String driver ="com.mysql.jdbc.Driver"; 
	public static String dbName ;
	public static String username, password;	
	public static String host;
	public static int port;	
	
	// this method we basically connect to the db
	// @return true or false to indicate connection success or fail 
	public static boolean connect(){
		boolean isConnect = false;
		// connection
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + dbName, username, password);
			isConnect = true;
		} catch (Exception e){
			System.err.println("Got an exception! @ connect()"); 
			e.printStackTrace();
			System.out.println("Can not connecto to databse");			
		}	
		return isConnect;
	}		
	// this method we basically disconnect from the db, closing statement, connection
	public static void disconnect(){
        try {
            conn.close();
            s.close();
        } catch (Exception e) {
			System.err.println("Got an exception! @ diconnect()");
			e.printStackTrace();
            System.err.println(e.getMessage());		            
        }		
		
	}	

	// this method implement select query which will fetch the result from db
	// @param take query statement as input
	// @return resultSet object
	public static ResultSet select(String query){
		ResultSet result = null;
		try{
		// first we create the statement 
		Statement s = conn.createStatement();
		result = s.executeQuery(query);
		} catch (Exception e){
			System.err.println("Got an exception! @ select()");
			e.printStackTrace();
            System.err.println(e.getMessage());				
		}
		return result;
	}	
	// this method will display the result of select query
	// @param take resultSet object as input
	// no return, but will print out result in tabular form
	public static void showSelect(ResultSet result){
		try {
		if (!result.isBeforeFirst()){
			System.out.println("no such data(tuple) exist in table");
			//return;
		}} catch(Exception e){
			System.err.println("Got an exception! @ showSelect()"); 
			e.printStackTrace();
            System.err.println(e.getMessage());				
		}
		if (result != null){
			try{
				// this class will contain the all the basic information of result
				ResultSetMetaData rsmd = (ResultSetMetaData) result.getMetaData();
				int noColumns = rsmd.getColumnCount();
				for (int i =0; i< noColumns; i++){
					System.out.format("%-25s"," | "+rsmd.getColumnName(i+1) + "/t ");		
				}
				System.out.println();
				// now we diaply the result here
				while(result.next()){
					// display the result
					for (int i =0; i < noColumns;i++){
						System.out.format("%-25s"," | "+result.getString(i+1));
					}
					System.out.println();
				}
			} catch (Exception e) {
				System.err.println("Got an exception! @ showSelect()"); 
				e.printStackTrace();
	            System.err.println(e.getMessage());			
			}
		} else {
			System.out.println("no such data(tuple) exist in table");
		}
		
	}	
	// this method implements query which is not select
	// ie. alter, create, insert, update, delete
	// @param take query statement as input
	// @return integer to indicate the query success or not, success: value; fail:-1   
	public static int query(String query){
		int result = -1;
		try {
			// first we create the statement
			Statement s = conn.createStatement();
			result = s.executeUpdate(query);
			//conn.close(); ==>No operations allowed after connection closed.
		} catch (Exception e){
			System.err.println("Got an exception! @ query()"); 
			e.printStackTrace();
            System.err.println(e.getMessage());			
		}
		return result;
	}	
	public static void main (String[] args){

		try {
			// Fetch args[0] to extract database logon and login info 
			String filename = args[0];			
			BufferedReader br = new BufferedReader (new FileReader(filename));
			String line; 	
			int counter = 0;	
			// extract database logon and login info
			while ((line=br.readLine()) != null){
				if (counter == 0){ host = line;
				} else if (counter == 1){ port =  Integer.parseInt(line);
				} else if (counter == 2){ dbName = line;
				} else if (counter == 3){ username = line;
				} else if (counter == 4){ password = line;
				} 
				counter++;
			}	
			br.close();
		} catch (Exception e) {
				System.err.println("Got an exception! @ main()"); 
				e.printStackTrace();
	            System.err.println(e.getMessage());				
		}
		// Fetch args[1]-args[6] .csv file for importing data  
		if (connect()){
			System.out.println("Connect to db "+dbName+" successfully !");
			try {
				// for input argument q1 which is for doing select query
				if (args[1].toLowerCase().equals("q1")) {
					int year = Integer.parseInt(args[2]);
					int month = Integer.parseInt(args[3]);
					// select
					String query = "select real_name, tag, nationality from players where birthday >= '"+year+"-"+month+"-01' and birthday <= '"+year+"-"+month+"-31'";
					System.out.println("Your Query Statement: "+query);
					showSelect(select(query));
				// for input argument q3 which is for doing select query	
				} else if (args[1].toLowerCase().equals("q2")) {
					// check if player exist in "new" team or not, yes and current: do nothing, no:insert a new one, yes and past: insert a new one
					int player = Integer.parseInt(args[2]);
					int team = Integer.parseInt(args[3]);	
					String query = "select * from members where player = "+player+" and team = "+team;
					ResultSet resultMembers = select(query);
					// test case 3
					if(!resultMembers.isBeforeFirst()){
						System.out.println("test case 3: not already a member of that team, add a new one");
						// insert a current "new" team member
						query = "insert members values("+player+", "+team+", now(), default)";
						query(query);	
						// test case 4
						// update all end_date of the current "another" team to now
						query = "select * from members where player = "+player+" and team != "+team+" and end_date is null";
						ResultSet resultAnotherTeamMembers = select(query);
						if (resultAnotherTeamMembers.isBeforeFirst()) {
							System.out.println("test case 4: presently a member of different team, membership record must be updated");
							query = "update members set end_date = now() where player = "+player+" and team != "+team+" and end_date is null";
							query(query);
						}						
					// test case 2	
					} else {
						ResultSetMetaData rsmdmem = (ResultSetMetaData) resultMembers.getMetaData();
						int size = rsmdmem.getColumnCount();					    
						if (size == 4) {
							System.out.println("test case 2:  a past member of the given ¡§new¡¨ team, add a new one");
							// insert a current "new" team member
							query = "insert members values("+player+", "+team+", now(), default)";
							query(query);
							// test case 4
							// update all end_date of the current "another" team to now
							query = "select * from members where player = "+player+" and team != "+team+" and end_date is null";
							ResultSet resultAnotherTeamMembers = select(query);
							if (resultAnotherTeamMembers.isBeforeFirst()) {
								System.out.println("test case 4: presently a member of different team, membership record must be updated");
								query = "update members set end_date = now() where player = "+player+" and team != "+team+" and end_date is null";
								query(query);
							}
						}
					}

				}
			} catch(Exception e) {
				System.err.println("Got an exception! @ args[] read"); 
				e.printStackTrace();
	            System.err.println(e.getMessage());					
			}	
		} else{
			System.out.println("Can not connect to db");
		}		
	}	
	
}
