package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class AnagraficaDealer extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {      0,                1,              14,              2,          3,        4,       5,          6,         7,         7,          7,           8          };
	private static String[] nameEdgesSecondaryVertexClass =   {"CODICE_FISCALE", "PARTITA_IVA", "RAGIONE_SOCIALE", "INDIRIZZO", "LOCALITA", "CAP", "PROVINCIA", "DSLOC", "TELEFONO", "TELEFONO", "TELEFONO", "CANALE_VENDITA"};
	
	public AnagraficaDealer(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT COD_ID, COD_ID_PADRE, CODICE_NEW, "
		           + " CODFIS, PIVA, RAGSOC, INDIRIZZO, LOCALITA, CAP, PROVINCIA, DSLOC, TEL1, TEL3, TEL4, CANALE"
		    + " FROM Kataskopeo_hash.ANAGRAFICA_DEALER";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String cod_id = resultSet.getString("COD_ID");
			String cod_id_padre = resultSet.getString("COD_ID_PADRE");
			String cod_new = resultSet.getString("CODICE_NEW");
			String primaryVertexClass = "CODICE_ANAGRAFICA_DEALER";
			String value = cod_id + " " + cod_id_padre;
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "value", value , "cod_id", cod_id, "cod_id_padre", cod_id_padre, "cod_new", cod_new);
			int j = 4;
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
