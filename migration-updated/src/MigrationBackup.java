package migrator;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;

public class MigrationBackup {

	static Connection connection_mysql = null;
	static PreparedStatement statement_mysql = null;
	
	public static void main(String[] args) {
		
		try {
			
			FileInputStream file = new FileInputStream(args[0]);
			Properties dbprop = new Properties();
			dbprop.load(file);			
			
			//ProcessBuilder pb = new ProcessBuilder("cmd", "/c", dbprop.getProperty("my_batch"));
			//Process p = pb.start();
			
			//gets path, username, password, key from file
			String mysql_connection_path = dbprop.getProperty("my_db_path") + dbprop.getProperty("my_port") + "/" + dbprop.getProperty("my_db_name") + "?zeroDateTimeBehavior=convertToNull";
			String username = dbprop.getProperty("my_username");
			String password = dbprop.getProperty("my_password");
			String aes_key = dbprop.getProperty("aes_key");
			
			//this is where the dump file gets created
			FileOutputStream fos = new FileOutputStream(dbprop.getProperty("output"));
			PrintStream ps = new PrintStream(fos);
			
			//System.out.println("waiting for mysql server to start...");
			//Thread.sleep(4000);
			
			//mysql database connection
			Class.forName("com.mysql.jdbc.Driver");
			connection_mysql = DriverManager.getConnection(mysql_connection_path, username, password);
			System.out.println("mysql database connected.");
			
			//extracting all existing tables from mysql
			statement_mysql = connection_mysql.prepareStatement("SHOW FULL TABLES WHERE Table_type != 'VIEW'");
			ResultSet rs_mysql_table = statement_mysql.executeQuery();
			
			//the real work starts here
			//loops every existing table
			//creates a dumpfile with query for 'create' and 'insert' from mysql database 
			while(rs_mysql_table.next()) {
				String table_name = rs_mysql_table.getString(1);
				
				//deletes table if any exists with the same name in postgres
				//drop_existing_table(table_name);
				
				//creates SQL query for each table
				String query_mysql = create_query(table_name);
				
				//System.out.println("generated 'create' query: " + query_mysql);
				
				//creates a dumpfile for generating 'create' query
				ps.println("--generated 'create' query statement");
				ps.println(query_mysql);
				
				//generated 'insert' query for tables with atleast one row
				statement_mysql = connection_mysql.prepareStatement("SELECT EXISTS (SELECT 1 FROM " + table_name + ");");
				ResultSet che = statement_mysql.executeQuery();
				che.next();
				int check = che.getInt(1);
				if(check == 1) {
					//creates SQL query for inserting data
					String data_mysql = data_query(table_name, aes_key);
					
					//System.out.println("generated 'insert' query: " + data_mysql);
						
					//updates dumpfile with 'insert' query
					ps.println("--generated 'insert' query statement");
					ps.println(data_mysql);
				}
				System.out.println("updated for " + table_name);
				
			}
			ps.close();
			System.out.println("dump file generated...");
			System.out.println("check " + dbprop.getProperty("output") + " for the file.");
			
		} catch(Exception e) {
			System.out.println("something went wrong...   " + e.getMessage());
		}
		
	}
	
	//this method generates query for creating table with postgres compatibility
	static String create_query(String table_name) throws SQLException {
		
		//all changes for create table query like BIT to BOOLEAN
		//BLOB to TEXT, AUTO_INCREMENT, PK, FK setting are done here
		
		//we use metadata to extract all information about columns
		statement_mysql = connection_mysql.prepareStatement("SELECT * FROM " + table_name);
		ResultSetMetaData rsmd_mysql_column = statement_mysql.getMetaData();
		
		int rsmd_mysql_column_count = rsmd_mysql_column.getColumnCount();
		StringBuilder query_mysql = new StringBuilder("CREATE TABLE " + table_name + "(");
		int count = 1;
		
		//for each column in a row
		while(count <= rsmd_mysql_column_count) {
			
			String mysql_column_name = rsmd_mysql_column.getColumnName(count);
			String mysql_column_type_name = rsmd_mysql_column.getColumnTypeName(count);
			
			//escapes sql variables
			if(mysql_column_name.equals("ORDER") || mysql_column_name.equals("OFFSET")) {
				mysql_column_name = "\"" + mysql_column_name + "\"";
			}
			
			query_mysql.append(mysql_column_name + " ");
			
			switch(mysql_column_type_name) {
			
			case "BIT" :											//bit to boolean conversion
				mysql_column_type_name = "BOOLEAN";
				break;
			
			case "DATETIME" :										//datetime to timestamp conversion
				mysql_column_type_name = "TIMESTAMP";
				break;
			
			case "DOUBLE" :											//double to float conversion
				mysql_column_type_name = "FLOAT";
				break;
				
			case "BLOB" :											//blob to bytea conversion
				mysql_column_type_name = "BYTEA";
				break;

			case "TINYINT" :
				mysql_column_type_name = "INT";
				break;
				
			}
			
			//sets size for varchar
			if(mysql_column_type_name.equals("VARCHAR")) {
				int size = rsmd_mysql_column.getPrecision(count);
				query_mysql.append(mysql_column_type_name + "(" + size +")" + ", ");
			} else {
				query_mysql.append(mysql_column_type_name + ", ");				
			}
			
			//sets auto increment
			if(rsmd_mysql_column.isAutoIncrement(count)) {
				query_mysql.setLength(query_mysql.length()-5);
				query_mysql.append("SERIAL, ");
			}
			
			//sets not null
			if(rsmd_mysql_column.isNullable(count) == 0) {
				query_mysql.setLength(query_mysql.length()-2);
				query_mysql.append(" NOT NULL, ");
			}
			
			count++;
		}
		
		//sets key constraints
		//from information schema we can obtain where pk enabled, fk and where it references
		statement_mysql = connection_mysql.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE table_schema = 'migration' and table_name = '" + table_name + "';");
		ResultSet rs_constraints = statement_mysql.executeQuery();
		while(rs_constraints.next()) {
			String constraint = rs_constraints.getString(3);
			if(constraint.equals("PRIMARY")) {
				query_mysql.append("PRIMARY KEY ("+ rs_constraints.getString(7) +"), ");
			} else {
				query_mysql.append("FOREIGN KEY (" + rs_constraints.getString(7)+") REFERENCES " + rs_constraints.getString(11) + "(" + rs_constraints.getString(12) + "), ");
			}
		}
		
		//final touch for generation query
		query_mysql.setLength(query_mysql.length()-2);
		query_mysql.append(");");
		
		return query_mysql.toString();
	}
	
	//this method generates query for inserting data with postgres compatibility
	static String data_query(String table_name, String aes_key) throws SQLException {
		
		//the changes like single quotes(') for text are done here and aes_decryption too
		statement_mysql = connection_mysql.prepareStatement("SELECT * FROM " + table_name);
		ResultSet rs_data = statement_mysql.executeQuery();
		ResultSetMetaData rsmd_data = statement_mysql.getMetaData();
		int column_size = rsmd_data.getColumnCount();
		StringBuilder data_query = new StringBuilder("");
		
		int row = 1;
		while(rs_data.next()) {
			
			data_query.append("INSERT INTO " + table_name + " VALUES(");
			int count = 1;
			
			while(count <= column_size) {
				
				String type = rs_data.getMetaData().getColumnTypeName(count);
				if(type.equals("INT") || type.equals("BIGINT")) {
					data_query.append(rs_data.getString(count) + ", ");
				}

				else if(type.equals("TINYINT")) {
					if(rs_data.getString(count) == null) {
						data_query.append("'0' ,");
					} else {
						data_query.append(rs_data.getString(count) + ", ");
					}
				}
				
				//null to 0000-00-00  00:00:00 timestamp
				else if(type.equals("TIMESTAMP")) {
					if(rs_data.getString(count) == null) {
						data_query.append("'1111-11-11 00:00:00',");
					}  else if(rs_data.getString(count).equals("0000-00-00 00:00:00")) {
						data_query.append("'1111-11-11 00:00:00',");
					} else {
						data_query.append("'" + rs_data.getString(count) + "', ");
					}
				}
				
				//null to timestamp and null to double and bit normalisation
				else if(type.equals("DATETIME") || type.equals("DOUBLE") || type.equals("BIT")) {
					if(rs_data.getString(count) == null) {
						data_query.append(null + ", ");
					} else if(rs_data.getString(count).equals("-1")) {
						data_query.append("'1', ");
					} else {
						data_query.append("'" + rs_data.getString(count) + "', ");
					}
				}
				
				//this statement decrypts aes encrypted password by mysterious way, it works! difficult to explain here
				else if(type.equals("BLOB") && !(table_name.equals("dae_actions"))) {
					statement_mysql = connection_mysql.prepareStatement("SELECT CAST(AES_DECRYPT(" + rs_data.getMetaData().getColumnName(count) + ", '" + aes_key + "') AS CHAR(50)) FROM " + table_name + ";");					
					ResultSet rs = statement_mysql.executeQuery();
					String password = null;
					int row_count = 0;
					while(rs.next() && row != row_count) {
						password = rs.getString(1);
						row_count++;
					}
					
					//pgp_sym_encrypt for encryption back the password with postgres compatibility
					data_query.append("PGP_SYM_ENCRYPT('" + password + "', '" + aes_key + "', 's2k-mode=1,cipher-algo=aes256'), ");
					row++;
				} 
				
				//corrects varchar and text
				else if(type.equals("VARCHAR") || type.equals("TEXT")) {
					String trimming = rs_data.getString(count);
					char ascii = 0;
					String trimmed = "null";
					try {
						trimmed = trimming.replaceAll("'", "''").replaceAll("\n", "").replaceAll("\r\n", "").replace(ascii, ' ');
						data_query.append("'" + trimmed + "', ");
					} catch(Exception e) {
						data_query.append("'" + rs_data.getString(count) + "', ");
					}
				}
				
				else {
					data_query.append("'" + rs_data.getString(count) + "', ");
				}
				count++;
			}
			
			data_query.setLength(data_query.length()-2);
			data_query.append(");");
			data_query.append("--eoi");
			data_query.append("\n");
		}
		
		//final touch
		data_query.setLength(data_query.length()-2);
		data_query.append(";");
		data_query.append("--eoi");
		
		return data_query.toString();
	}

}
