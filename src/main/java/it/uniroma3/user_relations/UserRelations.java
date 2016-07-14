package it.uniroma3.user_relations;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class UserRelations {

	private static OrientGraph kataskopeo;
	private static OrientGraph kataskopeoUserRelations;
	private static String VERTEX_CLASS_NAME = "User";
	private static String[] EDGE_CLASS_NAMES = {"SameTipologiaOrdine"};
	private static String SQL_SHORTEST_PATH = "SELECT expand(shortestPath( (SELECT * FROM ID_USER WHERE value=?), (SELECT * FROM ID_USER WHERE value=?), null, null, {'minDepth': 5, 'maxDepth': 5}))";

	@SuppressWarnings("static-access")
	public UserRelations(OrientGraphFactory kataskopeoGraphFactory, OrientGraphFactory kataskopeoUserRelationsGraphFactory) {
		this.kataskopeo = kataskopeoGraphFactory.getTx();
		this.kataskopeoUserRelations = kataskopeoUserRelationsGraphFactory.getTx();
		System.out.println("Init Schema");
		initSchema();
		System.out.println("Done");
		System.out.println("Import Users");
		populateKataskopeoUserRelations();
		System.out.println("Done");
		System.out.println("Create Relations");
		createRelations();
		System.out.println("Done");
	} 
	
	private void initSchema() {
		for (String edgeClass : EDGE_CLASS_NAMES) {
			OrientEdgeType edge = kataskopeoUserRelations.createEdgeType(edgeClass);
			edge.createProperty("value", OType.STRING);
		}
		OrientVertexType type = kataskopeoUserRelations.createVertexType(VERTEX_CLASS_NAME);
		type.createProperty("user_id", OType.STRING);
		kataskopeoUserRelations.createKeyIndex("user_id", Vertex.class, new Parameter<String, String>("class", VERTEX_CLASS_NAME));
		kataskopeoUserRelations.commit();
	}
	
	private void populateKataskopeoUserRelations() {
		OCommandSQL query = new OCommandSQL("SELECT * FROM ID_USER");
		Iterable<OrientVertex> user_ids = kataskopeo.command(query).execute();
		
		for (OrientVertex user : user_ids) {
			kataskopeoUserRelations.addVertex("class:"+VERTEX_CLASS_NAME, "user_id", user.getProperty("value"));
		}
		kataskopeoUserRelations.commit();
	}

	private void createRelations() {
		Iterable<Vertex> users = kataskopeo.getVerticesOfClass("ID_USER");
		Map<String, Map<String, Map<String, String>>> relations = new HashMap<String, Map<String, Map<String, String>>>();
		Iterator<Vertex> iterator = users.iterator();
		System.out.println("Find Relations");
		while(iterator.hasNext()){
			OrientVertex user = (OrientVertex) iterator.next();
			String firstUserID = user.getProperty("value");
			relations.put(firstUserID, sameTipologiaOrdine(firstUserID));
		}
		System.out.println("Create Relations");
		for (String firstUserID : relations.keySet()) {
			Iterable<Vertex> verticesFirstUser = kataskopeoUserRelations.getVertices(VERTEX_CLASS_NAME+".user_id", firstUserID);
			OrientVertex firstUser = (OrientVertex) verticesFirstUser.iterator().next();
			Map<String, Map<String, String>> mapTemp1 = relations.get(firstUserID);
			for (String secondUserID : mapTemp1.keySet()) {
				Map<String, String> mapTemp2 = mapTemp1.get(secondUserID);
				Iterable<Vertex> verticesSecondUser = kataskopeoUserRelations.getVertices(VERTEX_CLASS_NAME+".user_id", secondUserID);
				OrientVertex secondUser = (OrientVertex) verticesSecondUser.iterator().next();
				for (String edgeName : mapTemp2.keySet()) {
					String edgeValue = mapTemp2.get(edgeName);
					OrientEdge edge = kataskopeoUserRelations.addEdge("class:"+edgeName, firstUser, secondUser, edgeName);
					edge.setProperty("value", edgeValue);
				}
			}
			kataskopeoUserRelations.commit();
		}
	}

//	private void shortestPath(OrientGraph kataskopeo, OrientGraph kataskopeoUserRelations, OrientVertex userFirst, OrientVertex userSecond) {
//		String idFirst = userFirst.getProperty("user_id");
//		String idSecond = userSecond.getProperty("user_id");
//		OCommandSQL queryCommand = new OCommandSQL(SQL_SHORTEST_PATH);
//		Iterable<OrientVertex> result = kataskopeo.command(queryCommand).execute(idFirst, idSecond);
//		if(result.iterator().hasNext()) {
//			for (OrientVertex dio : result) {
//		
//				System.out.print(dio + " ");
//				
//				kataskopeoUserRelations.addEdge("class:ShortestPath", userFirst, userSecond, "ShortestPath");
//			}
//			System.out.println("");
//		} else {
//			System.out.println("DIO BONO");
//		}
//	}
	
	private Map<String, Map<String, String>> sameTipologiaOrdine(String firstUserID) {
		Map<String, Map<String, String>> relation = new HashMap<String, Map<String, String>>(); 
		OCommandSQL queryCommand = new OCommandSQL(
			    "TRAVERSE out('Has_CODICE_TESTATA_DWI'), out('Has_COD_TIPOLOGIA_ORDINE'), out('Has_CODICE_TESTATA_DWI'), out('Has_ID_CLIENTE') " 
			    +" FROM ("
			    +"   SELECT * "
			    +"     FROM ID_USER" 
			    +"    WHERE value=?"
			    +" )"
				);
		Iterable<OrientVertex> result = kataskopeo.command(queryCommand).execute(firstUserID);
		String cod_tipologia_ordine = "";
		Iterator<OrientVertex> resultIterator = result.iterator();
		while (resultIterator.hasNext()) {
			OrientVertex vertex = resultIterator.next();
			String vertexClass = vertex.getProperty("@class");
			if(vertexClass.equals("COD_TIPOLOGIA_ORDINE")){
				cod_tipologia_ordine = vertex.getProperty("value");
			}else if (vertexClass.equals("ID_USER")){
				String secondUserID = vertex.getProperty("value");
				if(!firstUserID.equals(secondUserID)){
					Map<String, String> property = new HashMap<String, String>();
					property.put("SameTipologiaOrdine", cod_tipologia_ordine);
					relation.put(secondUserID, property);
//					Iterator<Vertex> usersIterator = users.iterator();
//					OrientVertex secondUser = null;
//					while (usersIterator.hasNext()) {
//						secondUser = (OrientVertex) usersIterator.next();
//						if(secondUser.getProperty("user_id").equals(id)){
//							break;
//						}
//					}
//					OCommandSQL query = new OCommandSQL("SELECT * FROM "+VERTEX_CLASS_NAME+" WHERE user_id='"+id+"'");
//					Iterable<OrientVertex> vertices = kataskopeoUserRelations.command(query).execute();
					
//					Iterable<Vertex> vertices = kataskopeoUserRelations.getVertices(VERTEX_CLASS_NAME+".user_id", id);
//					OrientVertex secondUser = (OrientVertex) vertices.iterator().next();
					
//					Edge edge = kataskopeoUserRelations.addEdge("class:SameTipologiaOrdine", user, secondUser, "SameTipologiaOrdine");
//					edge.setProperty("Cod_Tipologia_Ordine", cod_tipologia_ordine);
//					kataskopeoUserRelations.commit();
				}
			}
		}
		return relation;
	}
	
}
