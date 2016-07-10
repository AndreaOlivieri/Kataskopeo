package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CanaleVendita extends Table {
	
	public CanaleVendita(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT ROWID_CRM, LINEA, PARTITA_IVA_CLIENTE, NUM_ALTERNATIVO_FISSO, NUM_ALTERNATIVO_MOBILE, CANALE, MACRO_CANALE, CAUSALE_FMS, SALES_COD_PARTNER, SALES_DES_PARTNER, INDIRIZZO_SEDE_IMPIANTO, CIVICO_DESE_IMPIANTO, PROVINCIA_SEDE_IMPIANTO, COMUNE_SEDE_IMPIANTO, CAP_SEDE_IMPIANTO, MARCAGGIO as MARCAGGIO_CLIENTE, COD_FISCALE_NASCITA as DATA_NASCITA_CLIENTE, COD_FISCALE_CITTA as CITTA_NASCITA_CLIENTE, COD_FISCALE_SESSO as SESSO_CLIENTE, COD_FISCALE_PROVINCIA as PROVINCIA_NASCITA_CLIENTE, COD_FISCALE_STATO as STATO_NASCITA_CLIENTE "
		    + " FROM Kataskopeo_hash.CANALE_VENDITA";
	}
	
	@Override
	protected void createClasses() {
		int columnCount;
		String columnName = "";
		try {
			columnCount = metaData.getColumnCount();
			graph.createVertexType("CODICE_CANALE_VENDITA");
			for (int i = 2; i <= columnCount; i++) {
				columnName = metaData.getColumnLabel(i);
				graph.createVertexType(columnName);
			}
			graph.createEdgeType("HasInCanaleVendita");
			graph.createEdgeType("HasOutCanaleVendita");
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
			String rowid_crm = resultSet.getString("ROWID_CRM");
			OrientVertex primaryVertex = graph.addVertex("class:CODICE_CANALE_VENDITA", "rowid_crm", rowid_crm);
			for (int i = 2; i <= columnCount; i++) {
			   columnName = metaData.getColumnLabel(i);
			   columnValue = resultSet.getString(columnName);
			   OrientVertex secondVertex = addDistinctVertex(columnName, columnValue);
			   graph.addEdge("class:HasInCanaleVendita", secondVertex, primaryVertex, "HasInCanaleVendita");
			   graph.addEdge("class:HasOutCanaleVendita", primaryVertex, secondVertex, "HasOutCanaleVendita");
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
