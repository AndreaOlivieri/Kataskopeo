package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class CFMSSosp extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {  };
	private static String[] nameEdgesSecondaryVertexClass =   { "NUMERO", "DATA_SOSPENSIONE" };
	
	public CFMSSosp(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT ID_SOSP,"
		           + " NUMERO, DATA_SOSPENSIONE"
		    + " FROM Kataskopeo_hash.CFMS_SOSP";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String id_sosp = resultSet.getString("ID_SOSP");
			String primaryVertexClass = "CODICE_CFMS_SOSP";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "id_sosp", id_sosp);
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
