package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class Device extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {  18,                 14,                            2,                              3,                          5,                              3,                  33,                            14,                         2,                           3,                     5,                        3            };  
	private static String[] nameEdgesSecondaryVertexClass =   {"IMEI", "RAGIONE_SOCIALE_DEALER_INGRESSO", "INDIRIZZO_DEALER_INGRESSO", "LOCALITA_DEALER_INGRESSO", "PROVINCIA_DEALER_INGRESSO", "COMUNE_DEALER_INGRESSO", "CODICE_DEALER_USCITA", "RAGIONE_SOCIALE_DEALER_USCITA", "INDIRIZZO_DEALER_USCITA", "LOCALITA_DEALER_USCITA", "PROVINCIA_DEALER_USCITA", "COMUNE_DEALER_USCITA" };
	
	public Device(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT IMEI, I_RAG_SOCIALE, I_INDIRIZZO, I_LOC, I_PR, I_COMUNE, O_DESTINATARIO_MERCI, O_RAG_SOCIALE, O_INDIRIZZO, O_LOC, O_PR, O_COMUNE "
		    + " FROM Kataskopeo_hash.DEVICE";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String imei = resultSet.getString("IMEI");
			String primaryVertexClass = "CODICE_DEVICE";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "imei", imei);			
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
