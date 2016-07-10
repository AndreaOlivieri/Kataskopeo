package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class AnagraficaDealer extends Table {
	
	public AnagraficaDealer(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT COD_ID, COD_ID_PADRE, CODICE_NEW, CODFIS as CODICE_FISCALE, PIVA as PARTITA_IVA, RAGSOC as RAGIONE_SOCIALE, INDIRIZZO, LOCALITA, CAP, PROVINCIA, DSLOC, CANALE"
		    + " FROM Kataskopeo_hash.ANAGRAFICA_DEALER";
	}
	
	@Override
	protected void createClasses() {
		int columnCount;
		String columnName = "";
		try {
			columnCount = metaData.getColumnCount();
			graph.createVertexType("CODICE_ANAGRAFICA_DEALER");
			for (int i = 4; i <= columnCount; i++) {
				columnName = metaData.getColumnLabel(i);
				OrientVertexType type = graph.createVertexType(columnName);
				type.createProperty("value", OType.STRING);
				graph.createKeyIndex("value", Vertex.class, new Parameter<String, String>("class", columnName));
			}
			graph.createEdgeType("HasInAnagraficaDealer");
			graph.createEdgeType("HasOutAnagraficaDealer");
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
			String cod_id = resultSet.getString("COD_ID");
			String cod_id_padre = resultSet.getString("COD_ID_PADRE");
			String cod_new = resultSet.getString("CODICE_NEW");
			OrientVertex primaryVertex = graph.addVertex("class:CODICE_ANAGRAFICA_DEALER", "cod_id", cod_id, "cod_id_padre", cod_id_padre, "codice_new", cod_new);
			for (int i = 4; i <= columnCount; i++) {
			   columnName = metaData.getColumnLabel(i);
			   columnValue = resultSet.getString(columnName);
			   OrientVertex secondVertex = addDistinctVertex(columnName, columnValue);
			   graph.addEdge("class:HasInAnagraficaDealer", secondVertex, primaryVertex, "HasInAnagraficaDealer");
			   graph.addEdge("class:HasOutAnagraficaDealer", primaryVertex, secondVertex, "HasOutAnagraficaDealer");
			}
			graph.commit();
		} catch (SQLException e) {
			graph.rollback();
			e.printStackTrace();
		}
	}
}
