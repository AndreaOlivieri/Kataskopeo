package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CanaleVendita extends Table {
	
	public CanaleVendita(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT ROWID_CRM, LINEA, PARTITA_IVA_CLIENTE, NUM_ALTERNATIVO_FISSO, NUM_ALTERNATIVO_MOBILE, CANALE, MACRO_CANALE, CAUSALE_FMS, SALES_COD_PARTNER, SALES_DES_PARTNER, INDIRIZZO_SEDE_IMPIANTO, CIVICO_DESE_IMPIANTO, PROVINCIA_SEDE_IMPIANTO, COMUNE_SEDE_IMPIANTO, CAP_SEDE_IMPIANTO, MARCAGGIO as MARCAGGIO_CLIENTE, COD_FISCALE_NASCITA as DATA_NASCITA_CLIENTE, COD_FISCALE_CITTA as CITTA_NASCITA_CLIENTE, COD_FISCALE_SESSO as SESSO_CLIENTE, COD_FISCALE_PROVINCIA as PROVINCIA_NASCITA_CLIENTE, COD_FISCALE_STATO as STATO_NASCITA_CLIENTE "
		    + " FROM Kataskopeo_hash.CANALE_VENDITA";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String rowid_crm = resultSet.getString("ROWID_CRM");
			String primaryVertexClass = "CODICE_CANALE_VENDITA";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "rowid_crm", rowid_crm);
			
			createLinkages(primaryVertex, secondaryVertexClasses[0], resultSet.getString(4),  "Has_"+metaData.getColumnName(4),  "Has_"+primaryVertexClass);
			createLinkages(primaryVertex, secondaryVertexClasses[1], resultSet.getString(5),  "Has_"+metaData.getColumnName(5),  "Has_"+primaryVertexClass);
			createLinkages(primaryVertex, secondaryVertexClasses[2], resultSet.getString(6),  "Has_"+metaData.getColumnName(6),  "Has_"+primaryVertexClass);
			createLinkages(primaryVertex, secondaryVertexClasses[3], resultSet.getString(7),  "Has_"+metaData.getColumnName(7),  "Has_"+primaryVertexClass);
			createLinkages(primaryVertex, secondaryVertexClasses[4], resultSet.getString(8),  "Has_"+metaData.getColumnName(8),  "Has_"+primaryVertexClass);
			createLinkages(primaryVertex, secondaryVertexClasses[5], resultSet.getString(9),  "Has_"+metaData.getColumnName(9),  "Has_"+primaryVertexClass);
			createLinkages(primaryVertex, secondaryVertexClasses[6], resultSet.getString(10), "Has_"+metaData.getColumnName(10), "Has_"+primaryVertexClass);
			createLinkages(primaryVertex, secondaryVertexClasses[7], resultSet.getString(11), "Has_"+metaData.getColumnName(11), "Has_"+primaryVertexClass);
			
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
