package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestataDWI extends Table {
	
	public TestataDWI(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT TO_V_COD_ROWID_ORD as ID_ORDINE, TO_V_COD_NUM_ORD as NUM_ORDINE, TO_V_DES_TIPO_CLI as TIPO_CLIENTE, TO_V_COD_ROWID_CLI as ID_CLIENTE, TO_V_COD_ACLI_CLI as COD_ACLI_CLIENTE, TO_V_COD_PAR_IVA_CLI as IVA_CLIENTE, TO_V_DES_NOME_CLI as NOME_CLIENTE, TO_V_DES_COGNOME_CLI as COGNOME_CLIENTE, TO_V_COD_RAG_SOC_CLI as RAGIONE_SOCIALE_CLIENTE, TO_V_COD_NUM_DOC_CLI as NUM_DOCUMENTO_CLIENTE, TO_V_COD_NUM_CNT as NUM_CONTRATTO_CLIENTE, TO_V_COD_CANALE_ORD as COD_TIPOLOGIA_ORDINE, TO_V_DES_IND_SEDE_LEG as INDIRIZZO_LEGALE_CLIENTE, TO_V_DES_COM_SEDE_LEG as COMUNE_LEGALE_CLIENTE, TO_V_DES_CAP_SEDE_LEG as CAP_LEGALE_CLIENTE, TO_V_DES_IND_SPED as INDIRIZZO_SPEDIZIONE_CLIENTE, TO_V_DES_COM_SPED as COMUNE_SPEDIZIONE_CLIENTE, ORD_V_COD_POS_ROWID_OWNER as ID_OWNER, TO_V_COD_TELEFONO_REF as TELEFONO_REFERENTE, TO_V_COD_CELLULARE_REF as CELLULARE_REFERENTE"
		    + " FROM Kataskopeo_hash.TESTATA_DWI";
	}
	
	@Override
	protected void createClasses() {
		int columnCount;
		String columnName = "";
		try {
			columnCount = metaData.getColumnCount();
			graph.createVertexType("CODICE_TESTATA_DWI");
			for (int i = 3; i <= columnCount; i++) {
				columnName = metaData.getColumnLabel(i);
				graph.createVertexType(columnName);
			}
			graph.createEdgeType("HasInTestataDWI");
			graph.createEdgeType("HasOutTestataDWI");
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
			String id_ordine = resultSet.getString("ID_ORDINE");
			String num_ordine = resultSet.getString("NUM_ORDINE");
			OrientVertex primaryVertex = graph.addVertex("class:CODICE_TESTATA_DWI", "id_ordine", id_ordine, "num_ordine", num_ordine);
			for (int i = 3; i <= columnCount; i++) {
			   columnName = metaData.getColumnLabel(i);
			   columnValue = resultSet.getString(columnName);
			   OrientVertex secondVertex = addDistinctVertex(columnName, columnValue);
			   graph.addEdge("class:HasInTestataDWI", secondVertex, primaryVertex, "HasInTestataDWI");
			   graph.addEdge("class:HasOutTestataDWI", primaryVertex, secondVertex, "HasOutTestataDWI");
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
