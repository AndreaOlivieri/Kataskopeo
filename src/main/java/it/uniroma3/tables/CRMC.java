package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CRMC extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {            0,                 15,       18,           7     };
	private static String[] nameEdgesSecondaryVertexClass =   { "CODICE_FISCALE_CLIENTE", "DEALER", "IMEI_APPATI", "MSISDN" };
	
	public CRMC(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT NUM_ORDINE,"
			       + " CF_CLIENTE, CONCAT(DEALER, ' ', DEALER), MOD_APPARATI, IMEI_APPATI, MSISDN"
		    + " FROM Kataskopeo_hash.CRMC"+ LIMIT_ROWS;
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String num_ordine = resultSet.getString("NUM_ORDINE");
			String primaryVertexClass = "CODICE_CRMC";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "num_ordine", num_ordine);
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
