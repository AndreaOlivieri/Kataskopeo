package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestataDWI extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {      27,              28,             29,                   1,                      14,                      16,                        30,                    31,                       2,                         3,                   4,                         2,                            3,                      32,              7,                      7          };  
	private static String[] nameEdgesSecondaryVertexClass =   {"NUMERO_ORDINE", "ID_CLIENTE", "COD_ACLI_CLIENTE", "PARTITA_IVA_CLIENTE", "RAGIONE_SOCIALE_CLIENTE", "NUM_DOCUMENTO_CLIENTE","NUM_CONTRATTO_CLIENTE", "COD_TIPOLOGIA_ORDINE", "INDIRIZZO_LEGALE_CLIENTE","COMUNE_LEGALE_CLIENTE", "CAP_LEGALE_CLIENTE","INDIRIZZO_SPEDIZIONE_CLIENTE", "COMUNE_SPEDIZIONE_CLIENTE", "OWNER_ORDINE", "TELEFONO_REFERENTE", "CELLULARE_REFERENTE"};
	
	public TestataDWI(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT TO_V_COD_ROWID_ORD,"
				+ " TO_V_COD_NUM_ORD, TO_V_COD_ROWID_CLI, TO_V_COD_ACLI_CLI, TO_V_COD_PAR_IVA_CLI, TO_V_COD_RAG_SOC_CLI, "
				+ " CASE TO_V_COD_NUM_DOC_CLI"
				+ "   WHEN '*ND' THEN NULL"
				+ "   WHEN 'N.D.' THEN NULL"
				+ "   ELSE TO_V_COD_NUM_DOC_CLI"
				+ " END,"
				+ " TO_V_COD_NUM_CNT, TO_V_COD_CANALE_ORD, TO_V_DES_IND_SEDE_LEG, TO_V_DES_COM_SEDE_LEG, TO_V_DES_CAP_SEDE_LEG, TO_V_DES_IND_SPED, TO_V_DES_COM_SPED, CONCAT(ORD_V_COD_POS_ROWID_OWNER,' ', POS_V_COD_POSTN_TYPE), TO_V_COD_TELEFONO_REF, TO_V_COD_CELLULARE_REF"
		    + " FROM Kataskopeo_hash.TESTATA_DWI"+ LIMIT_ROWS;
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String id_ordine = resultSet.getString("TO_V_COD_ROWID_ORD");		
			String primaryVertexClass = "CODICE_TESTATA_DWI";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "id_ordine", id_ordine);			
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
