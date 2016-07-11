package it.uniroma3.tables;

import java.sql.Connection;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestataDWI extends Table {
	
	private static int[] indexOccuranceSecondaryVertexClass = {  };
	private static String[] nameEdgesSecondaryVertexClass =   {"TIPO_CLIENTE", "ID_CLIENTE", "COD_ACLI_CLIENTE", "IVA_CLIENTE", "NOME_CLIENTE", "COGNOME_CLIENTE", "RAGIONE_SOCIALE_CLIENTE", "NUM_DOCUMENTO_CLIENTE","NUM_CONTRATTO_CLIENTE", "COD_TIPOLOGIA_ORDINE", "INDIRIZZO_LEGALE_CLIENTE","COMUNE_LEGALE_CLIENTE", "CAP_LEGALE_CLIENTE","INDIRIZZO_SPEDIZIONE_CLIENTE", "COMUNE_SPEDIZIONE_CLIENTE", "ID_OWNER", "TELEFONO_REFERENTE", "CELLULARE_REFERENTE"};
	
	public TestataDWI(Connection mysqlConnection, OrientGraphFactory orientDbFactory, String[] secondaryVertexClasses){
		super(mysqlConnection, orientDbFactory, secondaryVertexClasses);
	}
	
	@Override
	protected String sqlTable() {
		return "SELECT TO_V_COD_ROWID_ORD as ID_ORDINE, TO_V_COD_NUM_ORD as NUM_ORDINE" 
					 +"TO_V_DES_TIPO_CLI, TO_V_COD_ROWID_CLI, TO_V_COD_ACLI_CLI, TO_V_COD_PAR_IVA_CLI, TO_V_DES_NOME_CLI, TO_V_DES_COGNOME_CLI, TO_V_COD_RAG_SOC_CLI, TO_V_COD_NUM_DOC_CLI, TO_V_COD_NUM_CNT, TO_V_COD_CANALE_ORD, TO_V_DES_IND_SEDE_LEG, TO_V_DES_COM_SEDE_LEG, TO_V_DES_CAP_SEDE_LEG, TO_V_DES_IND_SPED, TO_V_DES_COM_SPED, ORD_V_COD_POS_ROWID_OWNER, TO_V_COD_TELEFONO_REF, TO_V_COD_CELLULARE_REF"
		    + " FROM Kataskopeo_hash.TESTATA_DWI";
	}
	
	@Override
	protected void createVertexesAndEdges() {
		try {
			String id_ordine = resultSet.getString("TO_V_COD_ROWID_ORD");
			String num_ordine = resultSet.getString("TO_V_COD_NUM_ORD");			
			String primaryVertexClass = "CODICE_TESTATA_DWI";
			OrientVertex primaryVertex = graph.addVertex("class:"+primaryVertexClass, "id_ordine", id_ordine, "num_ordine", num_ordine);			
			int j = 3;
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
