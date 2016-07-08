package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public abstract class Table {

	protected OrientGraph graph;
	protected ResultSet resultSet;
	protected ResultSetMetaData metaData;

	public Table(Connection mysqlConnection, OrientGraphFactory orientDbFactory) {
		graph = orientDbFactory.getTx();
		Statement statement;
		try {
			statement = mysqlConnection.createStatement();
			String sql = sqlTable();
			resultSet = statement.executeQuery(sql);
			metaData = resultSet.getMetaData();
			popolateGraph();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void popolateGraph() {
		try {
			System.out.println("Creating Class...");
			createClasses();
			System.out.print("Done");
			resultSet.first();
			System.out.print("Creating Vertexes and Edges");
			do {
				createVertexesAndEdges();
				System.out.print(".");
			} while(resultSet.next());
			System.out.print("Done");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	} 

	protected void printResultSet() {
		try {
			int columnCount = metaData.getColumnCount();
			resultSet.first();
			do {
				for (int i = 1; i <= columnCount; i++)
				{
					String columnName = metaData.getColumnLabel(i);
					String columnValue = resultSet.getString(columnName);
					System.out.print(columnName + ": " + columnValue + "\t");
				}
				System.out.println("");
			} while(resultSet.next());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	protected OrientVertex addDistinctVertex(String className, String value){
		graph.getVertices(className+".value", value);
		Iterable<Vertex> vertices = graph.getVertices(className+".value", value);
		OrientVertex temVert = null;
		if (vertices.iterator().hasNext()) {
			temVert = (OrientVertex) vertices.iterator().next();
		}else { 
			temVert = graph.addVertex("class:"+className, "value", value);
		}
		return temVert;
	}

	protected abstract String sqlTable();
	protected abstract void createClasses();
	protected abstract void createVertexesAndEdges();
}
