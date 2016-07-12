package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class DettaglioRDS extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {     27,             25};  
	private static String[] nameEdgesSecondaryVertexClass =   {"NUMERO_ORDINE", "CODICE_SERVIZIO"};
	
	public DettaglioRDS(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT DRS_V_COD_ROWID_RSM,"
				    + "DRS_V_COD_ROWID_ORD, DRS_V_COD_CUS_CTF"
		    + " FROM Kataskopeo_hash.DETTAGLIO_RDS";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String id_richiesta_servizio = resultSet.getString("DRS_V_COD_ROWID_RSM");
			String id_ordine = resultSet.getString("DRS_V_COD_ROWID_ORD");
			String value = id_richiesta_servizio + " " + id_ordine;
			String primaryVertexClass = "CODICE_DETTAGLIO_RDS";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass,"value", value, "id_richiesta_servizio", id_richiesta_servizio, "id_ordine", id_ordine);			
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
