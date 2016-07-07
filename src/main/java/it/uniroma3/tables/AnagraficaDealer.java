package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class AnagraficaDealer extends Table {
	
	public AnagraficaDealer(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
		//printResultSet(tableSet);
	}
	
	@Override
	protected void createVertexesAndEdges(ResultSetMetaData metaData, ResultSet resultSet) {
		String column_name = "";
		String column_value = "";
		OrientGraph graph = orientDbFactory.getTx();
		try {
			int columnCount = metaData.getColumnCount();
			String cod_id = resultSet.getString("COD_ID");
			String cod_id_padre = resultSet.getString("COD_ID_PADRE");
			String cod_new = resultSet.getString("CODICE_NEW");
			OrientVertex primaryVertex = graph.addVertex("class:ANAGRAFICA_DEALER_ID", "cod_id", cod_id, "cod_id_padre", cod_id_padre, "codice_new", cod_new);
			for (int i = 4; i <= columnCount; i++)
			{
			   column_name = metaData.getColumnLabel(i);
			   column_value = resultSet.getString(column_name);
			   OrientVertex secondVertex = graph.addVertex("class:"+column_name, "value", column_value);
			   graph.addEdge("class:HasInAnagraficaDealer", secondVertex, primaryVertex, "HasInAnagraficaDealer");
			   graph.addEdge("class:HasOutAnagraficaDealer", primaryVertex, secondVertex, "HasOutAnagraficaDealer");
			}
			graph.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected String sqlTable() {
		return "SELECT COD_ID, COD_ID_PADRE, CODICE_NEW, CODFIS, PIVA, RAGSOC, INDIRIZZO, LOCALITA, CAP, PROVINCIA, DSLOC, CANALE"
				+ " FROM Kataskopeo_hash.ANAGRAFICA_DEALER";
	}

}
