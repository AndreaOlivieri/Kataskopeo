package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CFMSAtt extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {       12,               15,              3,              12,              16,                  2,                     3,                   4      };
	private static String[] nameEdgesSecondaryVertexClass =   { "DATA_ATTIVAZIONE", "COD_DEALER", "LUOGO_NASCITA", "DATA_NASCITA", "NUMERO_DOCUMENTO", "INDIRIZZO_RESIDENZA", "COMUNE_RESIDENZA", "CAP_RESIDENZA" };
	
	public CFMSAtt(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT NUMERO,"
		           + " DATA_ATTIVAZIONE, CONCAT(COD_PDV, ' ', COD_DEALER), LUOGO_NASCITA, DATA_NASCITA, NUMERO_DOC, INDIRIZZO_RESIDENZA, COMUNE_RESIDENZA, CAP_RESIDENZA "
		    + " FROM Kataskopeo_hash.CFMS_ATT";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String numero = resultSet.getString("NUMERO");
			String primaryVertexClass = "CODICE_CFMS_ATT";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "numero", numero);
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
