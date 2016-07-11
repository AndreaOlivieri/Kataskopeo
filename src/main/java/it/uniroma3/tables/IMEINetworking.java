package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class IMEINetworking extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {  };
	private static String[] nameEdgesSecondaryVertexClass =   {"VISITOR_LOCATION_REGISTER", "NUMERO_TELEFONICO", "CODICE_LOCATION_AREA" };
	
	public IMEINetworking(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT IMSI, IMEI"   
					  +"VLR as VISITOR_LOCATION_REGISTER, MSISDN as NUMERO_TELEFONICO, LAC as CODICE_LOCATION_AREA "
			   + " FROM Kataskopeo_hash.IMEI_NETWORKING";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String imsi = resultSet.getString("IMSI");
			String imei = resultSet.getString("IMEI");
			String primaryVertexClass = "CODICE_IMEI_NETWORKING";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "imsi", imsi, "imei", imei);		
			int j = 2;
			String secondaryClassName = "";
			for (int i = 0; i < indexOccuranceSecondaryVertexClass.length; i++) {
				secondaryClassName = secondaryVertexClasses[indexOccuranceSecondaryVertexClass[i]];
				createLinkages(primaryVertex, secondaryClassName, resultSet.getString(j),  "Has_"+nameEdgesSecondaryVertexClass[i],  "Has_"+primaryVertexClass);
				j++;
				
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
