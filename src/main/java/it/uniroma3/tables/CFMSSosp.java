package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CFMSSosp extends Table {
	
	public CFMSSosp(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT ID_SOSP, NUMERO, DATA_SOSPENSIONE"
		    + " FROM Kataskopeo_hash.CFMS_SOSP";
	}
	
	@Override
	protected void createClasses() {
		int columnCount;
		String columnName = "";
		try {
			columnCount = metaData.getColumnCount();
			graph.createVertexType("CODICE_CFMS_SOSP");
			for (int i = 2; i <= columnCount; i++) {
				columnName = metaData.getColumnLabel(i);
				graph.createVertexType(columnName);
			}
			graph.createEdgeType("HasInCFMSSosp");
			graph.createEdgeType("HasOutCFMSSosp");
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
	
	@Override
	protected void createVertexesAndEdges() {
		String columnName = "";
		String columnValue = "";
		try {
			int columnCount = metaData.getColumnCount();
			String id_sosp = resultSet.getString("ID_SOSP");
			OrientVertex primaryVertex = graph.addVertex("class:CODICE_CFMS_SOSP", "id_sosp", id_sosp);
			for (int i = 2; i <= columnCount; i++) {
			   columnName = metaData.getColumnLabel(i);
			   columnValue = resultSet.getString(columnName);
			   OrientVertex secondVertex = addDistinctVertex(columnName, columnValue);
			   graph.addEdge("class:HasInCFMSSosp", secondVertex, primaryVertex, "HasInCFMSSosp");
			   graph.addEdge("class:HasOutCFMSSosp", primaryVertex, secondVertex, "HasOutCFMSSosp");
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
