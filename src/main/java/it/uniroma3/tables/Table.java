package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public abstract class Table {
	
	protected ResultSet tableSet;
	protected OrientGraphFactory orientDbFactory;
	
	public Table(Connection mysqlConnection, OrientGraphFactory orientDbFactory) {
		this.orientDbFactory = orientDbFactory;
		Statement statement;
		try {
			statement = mysqlConnection.createStatement();
			String sql = sqlTable();
			tableSet = statement.executeQuery(sql);
			popolateGraph(tableSet);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void popolateGraph(ResultSet resultSet) {
		ResultSetMetaData metaData;
		try {
			metaData = resultSet.getMetaData();
			resultSet.first();
			do {
				createVertexesAndEdges(metaData, resultSet);
			} while(resultSet.next());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	} 
	
	protected void printResultSet(ResultSet resultSet) {
		ResultSetMetaData metaData;
		try {
			metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			resultSet.first();
			do {
				for (int i = 1; i <= columnCount; i++)
				{
				   String column_name = metaData.getColumnLabel(i);
				   String column_value = resultSet.getString(column_name);
				   System.out.print(column_name + ": " + column_value + "\t");
				}
				System.out.println("");
			} while(resultSet.next());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	protected abstract String sqlTable();
	protected abstract void createVertexesAndEdges(ResultSetMetaData metaData, ResultSet resultSet);
}
