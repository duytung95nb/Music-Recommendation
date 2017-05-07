package CassandraDatabase;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class QueryExecute {
	private Connector connector;
	private String KEYSPACE = "moviedb"; // just like database name
	public static String TABLE_MOVIES = "modies";

	public QueryExecute(Connector connector) {
		this.connector = connector;
	}

	public ResultSet selectAll(String tableName) {
		String selectQuery = "SELECT * FROM " + this.KEYSPACE + "." + tableName;
		ResultSet result = this.connector.getSession().execute(selectQuery);
		return result;
	}

	// insert to columns with text type
	public ResultSet insert_text_value(String tableName, List<Entry<String, String>> insertData) {
		// insert
		StringBuilder columns = new StringBuilder();
		StringBuilder values = new StringBuilder();
		columns.append("(");
		values.append("(");
		// ,a,b,c,d,e
		for (Entry<String, String> entry : insertData) {
			columns.append("," + entry.getKey());
			values.append("," + "'" + entry.getValue() + "'");
		}
		values.append(")");
		values.deleteCharAt(1);
		columns.append(")");
		columns.deleteCharAt(1); // delete the first ','

		String insertQuery = "INSERT INTO " + this.KEYSPACE + "." + tableName + " " + columns.toString() + " VALUES "
				+ values.toString() + ";";
		System.out.println("Query string: " + insertQuery);
		ResultSet result = this.connector.getSession().execute(insertQuery);
		return result;
	}

	// insert to columns with integer type
	public ResultSet insert_int_value(String tableName, List<Entry<String, Integer>> insertData) {
		// insert
		StringBuilder columns = new StringBuilder();
		StringBuilder values = new StringBuilder();
		columns.append("(");
		values.append("(");
		// ,a,b,c,d,e
		for (Entry<String, Integer> entry : insertData) {
			columns.append("," + entry.getKey());
			values.append("," + entry.getValue());
		}
		values.append(")");
		values.deleteCharAt(1);
		columns.append(")");
		columns.deleteCharAt(1); // delete the first ','

		String insertQuery = "INSERT INTO " + this.KEYSPACE + "." + tableName + " " + columns.toString() + " VALUES "
				+ values.toString() + ";";
		System.out.println("Query string: " + insertQuery);
		ResultSet result = this.connector.getSession().execute(insertQuery);
		return result;
	}

	// public ResultSet insert(String tableName, Map<String, String> data){
	// //insert
	// System.out.println("prepared statement: ");
	// StringBuilder keyString = new StringBuilder();
	// keyString.append("(");
	// // ,a,b,c,d,e
	// for (String key : data.keySet()) {
	// keyString.append(","+key);
	// }
	// keyString.append(")");
	// keyString.deleteCharAt(1); // delete the first ','
	//
	// System.out.println("prepared statement: ");
	// StringBuilder preparedValues = new StringBuilder();
	// preparedValues.append("(");
	// if(data.size()>0){
	// preparedValues.append("?");
	// if(data.size()>1){
	// for (int i = 1; i < data.size(); i++) {
	// preparedValues.append(",?");
	// }
	// }
	// }
	// preparedValues.append(")");
	//
	// System.out.println("prepared keys: "+keyString.toString());
	// System.out.println("prepared values: "+preparedValues.toString());
	//
	// PreparedStatement prepared = this.connector.getSession().prepare(
	// "INSERT INTO "+this.KEYSPACE+ "."+tableName +" "
	// + keyString.toString() +" values "+ preparedValues.toString());
	// System.out.println("prepared statement: "+prepared.toString());
	// BoundStatement bound = prepared.bind(data.values());
	// ResultSet result = this.connector.getSession().execute(bound);
	// return result;
	// }
}
