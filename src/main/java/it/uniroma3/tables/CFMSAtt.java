package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CFMSAtt extends Table {
	
	public CFMSAtt(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT NUMERO, DATA_ATTIVAZIONE, COD_DEALER, COD_PDV as COD_PUNTO_VENDITA, MATRICOLA as MAT_DEALER, LUOGO_NASCITA, DATA_NASCITA, NUMERO_DOC as DOCUMENTO, INDIRIZZO_RESIDENZA as INDIRIZZO, COMUNE_RESIDENZA as COMUNE, CAP_RESIDENZA as CAP "
		    + " FROM Kataskopeo_hash.CFMS_ATT";
	}
	
	@Override
	protected void createClasses() {
		int columnCount;
		String columnName = "";
		try {
			columnCount = metaData.getColumnCount();
			graph.createVertexType("CODICE_CFMS_ATT");
			for (int i = 2; i <= columnCount; i++) {
				columnName = metaData.getColumnLabel(i);
				graph.createVertexType(columnName);
			}
			graph.createEdgeType("HasInCFMSAtt");
			graph.createEdgeType("HasOutCFMSAtt");
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
			String numero = resultSet.getString("NUMERO");
			OrientVertex primaryVertex = graph.addVertex("class:CODICE_CFMS_ATT", "numero", numero);
			for (int i = 2; i <= columnCount; i++) {
			   columnName = metaData.getColumnLabel(i);
			   columnValue = resultSet.getString(columnName);
			   OrientVertex secondVertex = addDistinctVertex(columnName, columnValue);
			   graph.addEdge("class:HasInCFMSAtt", secondVertex, primaryVertex, "HasInCFMSAtt");
			   graph.addEdge("class:HasOutCFMSAtt", primaryVertex, secondVertex, "HasOutCFMSAtt");
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
