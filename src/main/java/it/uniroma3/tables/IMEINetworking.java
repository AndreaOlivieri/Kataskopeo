package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class IMEINetworking extends Table {
	
	public IMEINetworking(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT IMSI, IMEI, VLR as VISITOR_LOCATION_REGISTER, MSISDN as NUMERO_TELEFONICO, LAC as CODICE_LOCATION_AREA "
		    + " FROM Kataskopeo_hash.IMEI_NETWORKING";
	}
	
	@Override
	protected void createClasses() {
		int columnCount;
		String columnName = "";
		try {
			columnCount = metaData.getColumnCount();
			graph.createVertexType("CODICE_IMEI_NETWORKING");
			for (int i = 3; i <= columnCount; i++) {
				columnName = metaData.getColumnLabel(i);
				graph.createVertexType(columnName);
			}
			graph.createEdgeType("HasInIMEINetworking");
			graph.createEdgeType("HasOutIMEINetworking");
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
			String cod_imsi = resultSet.getString("IMSI");
			String cod_imei = resultSet.getString("IMEI");			
			OrientVertex primaryVertex = graph.addVertex("class:CODICE_IMEI_NETWORKING", "cod_imsi", cod_imsi, "cod_imei", cod_imei);
			for (int i = 3; i <= columnCount; i++) {
			   columnName = metaData.getColumnLabel(i);
			   columnValue = resultSet.getString(columnName);
			   OrientVertex secondVertex = addDistinctVertex(columnName, columnValue);
			   graph.addEdge("class:HasInIMEINetworking", secondVertex, primaryVertex, "HasInIMEINetworking");
			   graph.addEdge("class:HasOutIMEINetworking", primaryVertex, secondVertex, "HasOutIMEINetworking");
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
