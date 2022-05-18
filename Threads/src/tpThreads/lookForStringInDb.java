package tpThreads;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class lookForStringInDb {
	public static void lookForStringInDb(String inputString, String nameDb) {
		List<String> ListTables = new ArrayList();//to stock names of the tables in the DB 

		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String urlMySql = "jdbc:mysql://localhost:3306/" + nameDb;
			String login = "root";
			String password = "";
			
			
			Connection conn = DriverManager.getConnection(urlMySql, login, password);
			
			DatabaseMetaData md = conn.getMetaData();
			String[] types = {"TABLE"};
			ResultSet rs = md.getTables(nameDb, null, "%", types);
			while (rs.next()) {
			  ListTables.add(rs.getString(3));
			}
			conn.close();
			
		}catch (ClassNotFoundException e) {
			System.out.println("Error: " + e.getMessage());
		}catch (SQLException e) {
			System.out.println("Error: " + e.getMessage());
		}
		
		List<Callable<String>> ListTask = new ArrayList();
		//for each column of a table be will create a task to look in the data of that column
		for(String table : ListTables) {
			int nbColonnes = 0;
			List<String> nomColonnes = new ArrayList();
			try {
				Class.forName("com.mysql.cj.jdbc.Driver");
				String urlMySql = "jdbc:mysql://localhost:3306/" + nameDb;
				String login = "root";
				String password = "";
				
				
				Connection conn = DriverManager.getConnection(urlMySql, login, password);
				
				Statement st = conn.createStatement();
				  
				String request = "SELECT * FROM " + table;
				ResultSet results = st.executeQuery(request);
				  
				ResultSetMetaData rsmd = results.getMetaData();
				
				nbColonnes = rsmd.getColumnCount();
				
				for (int i = 1; i <= nbColonnes; i++ ) {
					String nom = rsmd.getColumnName(i);
					nomColonnes.add(nom);
				}
				conn.close();
				
			}catch (ClassNotFoundException e) {
				System.out.println("Error: " + e.getMessage());
			}catch (SQLException e) {
				System.out.println("Error: " + e.getMessage());
			}

				for (int i = 1; i <= nbColonnes; i++ ) {
					int j = i;
					Callable<String> task = () -> {
						try {
							Class.forName("com.mysql.cj.jdbc.Driver");
							String urlMySql = "jdbc:mysql://localhost:3306/" + nameDb;
							String login = "root";
							String password = "";
							
							
							Connection conn = DriverManager.getConnection(urlMySql, login, password);
							
							Statement st = conn.createStatement();
							  
							String request = "SELECT " + "*" + " FROM " + table;
							ResultSet results = st.executeQuery(request);
							int compteurLigne = 1;
							while(results.next()){ 
								if (inputString.equals(results.getString(j))) 
									return table + "-" + nomColonnes.get(j-1) + "-Ligne: " + compteurLigne;
								compteurLigne++;
								}
							
							
							conn.close();
							
						}catch (ClassNotFoundException e) {
							System.out.println("Error: " + e.getMessage());
						}catch (SQLException e) {
							System.out.println("Error: " + e.getMessage());
						}
						return ""; 
						
					};
					ListTask.add(task);
				}
		}
		
		ExecutorService executor = Executors.newCachedThreadPool();
		
		try {
			List<Future<String>> futures = executor.invokeAll(ListTask);
			for(Future<String> future : futures) {
				if (future.get().equals(""))
					continue;
				else System.out.println(future.get());
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main (String[] args) {
		lookForStringInDb("haytham", "mobilite_international");
	}
}
