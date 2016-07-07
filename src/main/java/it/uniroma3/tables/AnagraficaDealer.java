package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class AnagraficaDealer extends Table {
	
	public AnagraficaDealer(Connection mysqlConnection, OrientGraphFactory orientDbFactory){
		super(mysqlConnection, orientDbFactory);
		//printResultSet(tableSet);
	}
	
	@Override
	protected void createVertexesAndEdges(ResultSetMetaData metaData, ResultSet resultSet) {
		OrientGraph graph = orientDbFactory.getTx();
		//ciao
		try {
			int columnCount = metaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++)
			{
			   String column_name = metaData.getColumnLabel(i);
			   String column_value = resultSet.getString(column_name);
			   graph.addVertex("class:"+column_name, "value", column_value);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected String sqlTable() {
		return "SELECT * FROM Kataskopeo_hash.ANAGRAFICA_DEALER";
	}

}
