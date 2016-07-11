package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OrdinativiFisso extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {    22,           1,             23,              24,               12,              25,                 26,                8,               7,                   2,              3,          3,        4,          16         };
	private static String[] nameEdgesSecondaryVertexClass =   { "ID_USER", "PARTITA_IVA", "TIPO_CLIENTE", "TIPO_SERVIZIO_CRM","DATA_ORDINE", "CODICE_SERVIZIO", "CODICE_SEGMENTO", "CANALE_VENDITA", "TELEFONO_CLIENTE", "INDIRIZZO_CLIENTE", "COMUNE", "PROVINCIA", "CAP", "NUMERO_DOCUMENTO"};
	
	public OrdinativiFisso(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT ID_ORDINATIVO "  
			        +" ID_USER, PARTITA_IVA, TIPO_CLIENTE, TIPO_SERVIZIO_CRM, DATA_EMISSIONE, CODICE_SERVIZIO, SEGMENTO, CANALE_VENDITA, CONCAT(PREFISSO,' ', NUMERO), CONCAT(VIA, ' ', CIVICO), COMUNE, PROVINCIA, CAP, NUMERO_DOCUMENTO"
		    + " FROM Kataskopeo_hash.ORDINATIVI_FISSO";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String id_ordinativo = resultSet.getString("ID_ORDINATIVO");
			String primaryVertexClass = "CODICE_ORDINATIVI_FISSO";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "id_ordinativo", id_ordinativo);
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
