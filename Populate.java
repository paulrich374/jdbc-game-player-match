
import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;

import java.io.InputStreamReader;
import java.io.FileInputStream;


public class Populate {
	
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
	
	
	
	// this methods implement import data from .csv file into database table
	// @param filename(could be file directory)
	public static void insertTuples(String filename){
		try {
			//BufferedReader br = new BufferedReader (new FileReader(filename));
			
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF8"));

			
			
			
			// extract table name
			String tableName="";
			int mid= filename.lastIndexOf(".");
			tableName=filename.substring(0,mid);
			String line; 
			PreparedStatement pst = null;
			System.out.println("Start importing data to table "+tableName+" from "+filename+" !");
			// insert data from .csv file line by line
			while ((line=br.readLine()) != null){
				String[] val = line.split(",");//separator
				String sql = "";
				int temp = 0;
				String nameEncode = val[2].replaceAll("'","''");
				//nameEncode = new String(nameEncode.getBytes("UTF-8"),"UTF-8");
				// if .csv is for table Players
				if (tableName.toLowerCase().equals("players")) {
					sql ="insert into players (player_id, tag, real_name, nationality, birthday, game_race)"
						+ "values ('"+val[0]+"','"+val[1]+"','"+nameEncode+"','"+val[3]+"','"+val[4]+"','"+val[5]+"')"; 
				}               
				
				// if .csv is for table Teams
				if (tableName.toLowerCase().equals("teams")) {
					// for tuples with empty values, it will not be fetched
					if (val.length == 3 ) 
					{
						sql ="insert into teams (team_id, name, founded)"
							+ "values ('"+val[0]+"','"+val[1].replaceAll("'","''")+"','"+val[2]+"')";
					} else {
						sql ="insert into teams (team_id, name, founded, disbanded)"
							+ "values ('"+val[0]+"','"+val[1].replaceAll("'","''")+"','"+val[2]+"','"+val[3]+"')";					
					}
				}
						
				// if .csv is for table Members
				if (tableName.toLowerCase().equals("members")) {
					// for tuples with empty values, it will not be fetched
					if (val.length == 3 ) 
					{
						sql ="insert into members (player, team, start_date)"
							+ "values ('"+val[0]+"','"+val[1]+"','"+val[2]+"')";
					} else {
						sql ="insert into members (player, team, start_date, end_date)"
							+ "values ('"+val[0]+"','"+val[1]+"','"+val[2]+"','"+val[3]+"')";					
					}
				}				
						
				// if .csv is for table Tournaments
				if (tableName.toLowerCase().equals("tournaments")) {
					// for tuples with empty values, it will not be fetched
					if (val.length == 3 ) 
					{
						// for tuple value is true or false, we convert to 1 and 0 for our tinyint type
						temp = (val[2].equals("TRUE"))?1:0;
						sql ="insert into tournaments (tournament_id, name, major)"
							+ "values ('"+val[0]+"','"+val[1].replaceAll("'","''")+"','"+temp+"')";
					} else {
						// for tuple value is true or false, we convert to 1 and 0 for our tinyint type
						temp = (val[3].equals("TRUE"))?1:0;
						sql ="insert into tournaments (tournament_id, name, region, major)"
							+ "values ('"+val[0]+"','"+val[1].replaceAll("'","''")+"','"+val[2]+"','"+temp+"')";					
					}
				}
				// if .csv is for table Matches
				if (tableName.toLowerCase().equals("matches")) {
					// for tuple value is true or false, we convert to 1 and 0 for our tinyint type
					temp = (val[7].equals("TRUE"))?1:0;
					sql ="insert into matches (match_id, date, tournament, playerA, playerB, scoreA, scoreB, offline)"
							+ "values ('"+val[0]+"','"+val[1]+"','"+val[2]+"','"+val[3]+"','"+val[4]+"','"+val[5]+"','"+val[6]+"','"+temp+"')";	
				}			
				
				// if .csv is for table Earnings
				if (tableName.toLowerCase().equals("earnings")) {	
					sql ="insert into earnings (tournament, player, prize_money, position)"
						+ "values ('"+val[0]+"','"+val[1]+"','"+val[2]+"','"+val[3]+"')";
				}
				// execute insert statement
				pst = conn.prepareStatement(sql);
				pst.executeUpdate();

			}
			System.out.println("Importing data to table "+tableName+" successfully !");
			br.close();
		} catch (Exception e) {
			System.err.println("Got an exception! @ insertTuples()"); 
			e.printStackTrace();
            System.err.println(e.getMessage());				
		}
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
			for (int i = 1; i < 7;i++) {
				insertTuples(args[i]);
			}			
		} else{
			System.out.println("Can not connect to db");
		}		
	}
}