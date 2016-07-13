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
	protected String[] secondaryVertexClasses;
	
	protected static String LIMIT_ROWS = "";

	public Table(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses) {
		this.secondaryVertexClasses = secondaryVertexClasses;
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
			resultSet.first();
			do {
				createVertexesAndEdges();
			} while(resultSet.next());
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

	protected void createLinkages(OrientVertex primaryVertex, String secondaryClassName, String secondaryValue, String primary2secondaryEdgeName, String secondary2primaryEdgeName) {
		if (secondaryValue!=null && !secondaryValue.isEmpty()) {
			OrientVertex secondaryVertex = addDistinctVertex(secondaryClassName, secondaryValue);
			graph.addEdge("class:"+secondary2primaryEdgeName, secondaryVertex, primaryVertex, secondary2primaryEdgeName);
			graph.addEdge("class:"+primary2secondaryEdgeName, primaryVertex, secondaryVertex, primary2secondaryEdgeName);
		}
	}

	protected OrientVertex addDistinctVertex(String className, String value){
		OrientVertex temVert = null;
		value = value.toLowerCase();
		Iterable<Vertex> vertices = graph.getVertices(className+".value", value);
		if (vertices.iterator().hasNext()) {
			temVert = (OrientVertex) vertices.iterator().next();
		}else { 
			temVert = graph.addVertex("class:"+className, "value", value);
		}
		return temVert;
	}

	protected abstract String sqlTable();
	protected abstract void createVertexesAndEdges();
}
