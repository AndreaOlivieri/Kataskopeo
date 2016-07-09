package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CRMC extends Table {
	
	public CRMC(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT NUM_ORDINE, NOME, COGNOME, CF_CLIENTE, DEALER, MOD_APPARATI, IMEI_APPATI, MSISDN as NUM_UTENTE"
		    + " FROM Kataskopeo_hash.CRMC";
	}
	
	@Override
	protected void createClasses() {
		int columnCount;
		String columnName = "";
		try {
			columnCount = metaData.getColumnCount();
			graph.createVertexType("CODICE_CRMC");
			for (int i = 2; i <= columnCount; i++) {
				columnName = metaData.getColumnLabel(i);
				graph.createVertexType(columnName);
			}
			graph.createEdgeType("HasInCRMC");
			graph.createEdgeType("HasOutCRMC");
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
			String num_ordine = resultSet.getString("NUM_ORDINE");
			OrientVertex primaryVertex = graph.addVertex("class:CODICE_CRMC", "num_ordine", num_ordine);
			for (int i = 2; i <= columnCount; i++) {
			   columnName = metaData.getColumnLabel(i);
			   columnValue = resultSet.getString(columnName);
			   OrientVertex secondVertex = addDistinctVertex(columnName, columnValue);
			   graph.addEdge("class:HasInCRMC", secondVertex, primaryVertex, "HasInCRMC");
			   graph.addEdge("class:HasOutCRMC", primaryVertex, secondVertex, "HasOutCRMC");
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
