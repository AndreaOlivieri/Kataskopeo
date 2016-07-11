package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CanaleVendita extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {  9,           1,                        7,                         7,                  8,                    8,                    10,                     2,                          3,                        3,                    4,                 11,                   12,                     3,                          5,                        13               };
	private static String[] nameEdgesSecondaryVertexClass =   {"LINEA", "PARTITA_IVA_CLIENTE", "NUM_ALTERNATIVO_FISSO", "NUM_ALTERNATIVO_MOBILE", "CANALE_VENDITA", "MACRO_CANALE_VENDITA", "SALES_COD_PARTNER", "INDIRIZZO_SEDE_IMPIANTO", "PROVINCIA_SEDE_IMPIANTO", "COMUNE_SEDE_IMPIANTO", "CAP_SEDE_IMPIANTO", "MARCAGGIO_CLIENTE", "DATA_NASCITA_CLIENTE", "CITTA_NASCITA_CLIENTE", "PROVINCIA_NASCITA_CLIENTE", "STATO_NASCITA_CLIENTE" };
	
	public CanaleVendita(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT ROWID_CRM, "
				    + "LINEA, PARTITA_IVA_CLIENTE, NUM_ALTERNATIVO_FISSO, NUM_ALTERNATIVO_MOBILE, CANALE, MACRO_CANALE, SALES_COD_PARTNER, INDIRIZZO_SEDE_IMPIANTO, CONCAT(INDIRIZZO_SEDE_IMPIANTO,' ', CIVICO_SEDE_IMPIANTO), COMUNE_SEDE_IMPIANTO, CAP_SEDE_IMPIANTO, MARCAGGIO, COD_FISCALE_NASCITA, COD_FISCALE_CITTA, COD_FISCALE_PROVINCIA, COD_FISCALE_STATO "
		    + " FROM Kataskopeo_hash.CANALE_VENDITA";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String rowid_crm = resultSet.getString("ROWID_CRM");
			String primaryVertexClass = "CODICE_CANALE_VENDITA";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "rowid_crm", rowid_crm);
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
