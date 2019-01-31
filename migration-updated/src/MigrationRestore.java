package migrator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.Scanner;

public class MigrationRestore {

	static Connection connection_postgres = null;
	static PreparedStatement statement_postgres = null;
	
	public static void main(String[] args) {
		
		try {
			
			FileInputStream file = new FileInputStream(args[0]);
			Properties dbprop = new Properties();
			dbprop.load(file);
			
			//ProcessBuilder pb = new ProcessBuilder("cmd", "/c", dbprop.getProperty("post_batch"));
			//Process p = pb.start();
			
			FileOutputStream err = new FileOutputStream(dbprop.getProperty("error"));
			PrintStream pe = new PrintStream(err);
			
			String file_path = dbprop.getProperty("output");
			String postgres_connection_path = dbprop.getProperty("post_db_path") + dbprop.getProperty("post_port") + "/" + dbprop.getProperty("post_db_name");
			String username = dbprop.getProperty("post_username");
			String password = dbprop.getProperty("post_password");
			
			//System.out.println("waiting for postgres server to start...");
			//Thread.sleep(4000);
			
			//postgres database connection
			Class.forName("org.postgresql.Driver");
			connection_postgres = DriverManager.getConnection(postgres_connection_path, username, password);
			System.out.println("postgres database connected.");
			
			String queries = null;
			
			/*
			while((queries = brr.readLine()) != null) {
				
				try {
				statement_postgres = connection_postgres.prepareStatement(queries);
				statement_postgres.execute();
				System.out.println(queries);
				} catch(Exception e) {
					pe.println(queries + "   " + e.getMessage());
				}
			}
			brr.close();
			*/
			Scanner scanner = new Scanner(new File(file_path));
			scanner.useDelimiter("--eoi");
			
			while(scanner.hasNext()) {
				try {
					queries = scanner.next();
					System.out.println(queries);
					statement_postgres = connection_postgres.prepareStatement(queries);
					statement_postgres.execute();
				} catch(Exception e) {
					pe.println(queries + "   " + e.getMessage());
				}
			}
			System.out.println("Database migrated successfully.");
			pe.close();
			
		} catch(Exception e) {
			System.out.println("something went wrong...  " + e.getMessage());
		}

	}

}
