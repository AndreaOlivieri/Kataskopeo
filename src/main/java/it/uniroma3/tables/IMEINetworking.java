package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class IMEINetworking extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {  19,   18,            20,                  7,               21           };
	private static String[] nameEdgesSecondaryVertexClass =   {"IMSI, IMEI, VISITOR_LOCATION_REGISTER", "MSISDN", "CODICE_LOCATION_AREA" };
	
	public IMEINetworking(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT IMSI, IMEI, VLR, MSISDN, LAC "
			   + " FROM Kataskopeo_hash.IMEI_NETWORKING";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String imsi = resultSet.getString("IMSI");
			String imei = resultSet.getString("IMEI");
			String value = imsi + " " + imei;
			String primaryVertexClass = "CODICE_IMEI_NETWORKING";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "value", value, "imsi", imsi, "imei", imei);		
			int j = 1;
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
