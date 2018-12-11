package migration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MySQLtoPostgreSQL {
	
	static Connection connection_mysql = null;
	static PreparedStatement statement_mysql = null;
	
	static Connection connection_postgres = null;
	static PreparedStatement statement_postgres = null;
	
	static File file = new File("E:\\migration\\key\\aes_key.txt");		//save your key at this location or copy your key location here

	public static void main(String[] args) {
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(file));
			String aes_key = null;
			aes_key = br.readLine();
			br.close();
			//mysql database connection
			Class.forName("com.mysql.cj.jdbc.Driver");
			connection_mysql = DriverManager.getConnection("jdbc:mysql://localhost:3306/migration", "root", "ch3coona");
			System.out.println("mysql database connected");
			
			//postgres database connection
			Class.forName("org.postgresql.Driver");
			connection_postgres = DriverManager.getConnection("jdbc:postgresql://localhost:5432/migration", "postgres", "ch3coona");
			System.out.println("postgres database connected");
			
			//extracting all existing tables from mysql
			statement_mysql = connection_mysql.prepareStatement("SHOW FULL TABLES WHERE Table_type != 'VIEW'");
			ResultSet rs_mysql_table = statement_mysql.executeQuery();
			
			//the real work starts here
			//for every table in mysql, this block creates query for creating table
			//inserting data with all constraints and the executes the query
			//so as to insert the data in postgres database
			while(rs_mysql_table.next()) {
				String table_name = rs_mysql_table.getString(1);
				
				//deletes table if any exists with the same name in postgres
				//drop_existing_table(table_name);
				
				//creates SQL query for each table
				String query_mysql = create_query(table_name);
				
				System.out.println("generated 'create' query: " + query_mysql);
///*				
				//create tables in postgres
				statement_postgres = connection_postgres.prepareStatement(query_mysql);
				statement_postgres.execute();
//*/				
				//creates SQL query for inserting data
				String data_mysql = data_query(table_name, aes_key);
				
				System.out.println("generated 'insert' query: " + data_mysql);
///*				
				//insert data in postgres
				statement_postgres = connection_postgres.prepareStatement(data_mysql);
				statement_postgres.execute();
//*/				
				System.out.println("'" + table_name + "' migrated successfully...");
			}
			
			
		} catch(Exception e) {
			System.out.println("error... " + e.getMessage()); //no one cares
		}
		

	}
	
	static void drop_existing_table(String table_name) throws SQLException{
		
		System.out.println("dropping the table '" + table_name + "' if exists in postgres...");
		statement_postgres = connection_postgres.prepareStatement("DROP TABLE IF EXISTS " + table_name);
		statement_postgres.execute();
		
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
		
		while(count <= rsmd_mysql_column_count) {
			
			String mysql_column_name = rsmd_mysql_column.getColumnName(count);
			String mysql_column_type_name = rsmd_mysql_column.getColumnTypeName(count);
			
			query_mysql.append(mysql_column_name + " ");
			
			//changes bit to boolean
			if(mysql_column_type_name.equals("BIT")) {
				mysql_column_type_name = "BOOLEAN";
			}
			
			//changes blob to text
			if(mysql_column_type_name.equals("BLOB")) {
				mysql_column_type_name = "BYTEA";
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
		StringBuilder data_query = new StringBuilder("INSERT INTO " + table_name + " VALUES(");
		
		int row = 1;
		while(rs_data.next()) {
			int count = 1;
			while(count <= column_size) {
				String type = rs_data.getMetaData().getColumnTypeName(count);
				if(type.equals("INT")) {
					data_query.append(rs_data.getString(count) + ", ");
				} 
				
				//this statement decrypts aes encrypted password by mysterious way, it works! difficult to explain here
				else if(type.equals("BLOB")) {
					statement_mysql = connection_mysql.prepareStatement("SELECT CAST(AES_DECRYPT(" + rs_data.getMetaData().getColumnName(count) + ", '" + aes_key + "') AS CHAR(50)) FROM " + table_name + ";");					
					ResultSet rs = statement_mysql.executeQuery();
					String password = null;
					int row_count = 0;
					while(rs.next() && row != row_count) {
						password = rs.getString(1);
						row_count++;
					}
					
					//pgp_sym_encrypt for encryption back the password with postgres compatibility
					data_query.append("PGP_SYM_ENCRYPT('" + password + "', '" + aes_key + "'), ");
					row++;
				} else {
					data_query.append("'" + rs_data.getString(count) + "', ");
				}
				count++;
			}
			
			data_query.setLength(data_query.length()-2);
			data_query.append("),(");
		}
		
		//final touch
		data_query.setLength(data_query.length()-2);
		data_query.append(";");
		
		return data_query.toString();
	}
	
}
