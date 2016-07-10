package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OrdinativiFisso extends Table {
	
	public OrdinativiFisso(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT ID_ORDINATIVO, ID_CENTRALE, ID_USER, ID_AREA_CENTRALE_USER, PARTITA_IVA, TIPO_CLIENTE, TIPO_SERVIZIO_CRM, DATA_EMISSIONE as DATA_ORDINE, CODICE_SERVIZIO, SEGMENTO as CODICE_SEGMENTO, CANALE_VENDITA, PREFISSO as PREFISSO_CLIENTE, NUMERO as NUM_CLIENTE, AREA_CENTRALE as CODICE_AREA_CENTRALE, VIA, CIVICO, COMUNE, PROVINCIA, CAP, NUMERO_DOCUMENTO"
		    + " FROM Kataskopeo_hash.ORDINATIVI_FISSO";
	}
	
	@Override
	protected void createClasses() {
		int columnCount;
		String columnName = "";
		try {
			columnCount = metaData.getColumnCount();
			graph.createVertexType("CODICE_ORDINATIVI_FISSO");
			for (int i = 2; i <= columnCount; i++) {
				columnName = metaData.getColumnLabel(i);
				graph.createVertexType(columnName);
			}
			graph.createEdgeType("HasInOrdinativiFisso");
			graph.createEdgeType("HasOutOrdinativiFisso");
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
			String id_ordinativo = resultSet.getString("ID_ORDINATIVO");
			OrientVertex primaryVertex = graph.addVertex("class:CODICE_ORDINATIVI_FISSO", "id_ordinativo", id_ordinativo);
			for (int i = 2; i <= columnCount; i++) {
			   columnName = metaData.getColumnLabel(i);
			   columnValue = resultSet.getString(columnName);
			   OrientVertex secondVertex = addDistinctVertex(columnName, columnValue);
			   graph.addEdge("class:HasOrdinativiFisso", secondVertex, primaryVertex, "HasInOrdinativiFisso");
			   graph.addEdge("class:HasOutOrdinativiFisso", primaryVertex, secondVertex, "HasOutOrdinativiFisso");
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
