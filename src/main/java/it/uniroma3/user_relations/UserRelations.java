package it.uniroma3.user_relations;

import java.util.Iterator;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class UserRelations {

	private static OrientGraphFactory kataskopeoGraphFactory;
	private static OrientGraphFactory kataskopeoUserRelationsGraphFactory;
	private static String VERTEX_CLASS_NAME = "User";
	private static String[] EDGE_CLASS_NAMES = {"ShortestPath"};
	private static String SQL_SHORTEST_PATH = "SELECT expand(shortestPath( (SELECT * FROM ID_USER WHERE value=?), (SELECT * FROM ID_USER WHERE value=?), null, null, {'minDepth': 5, 'maxDepth': 5}))";

	@SuppressWarnings("static-access")
	public UserRelations(OrientGraphFactory kataskopeoGraphFactory, OrientGraphFactory kataskopeoUserRelationsGraphFactory) {
		this.kataskopeoGraphFactory = kataskopeoGraphFactory;
		this.kataskopeoUserRelationsGraphFactory = kataskopeoUserRelationsGraphFactory;
		initSchema();
		populateKataskopeoUserRelations();
		createRelations();
	} 
	
	private void initSchema() {
		OrientGraphNoTx kataskopeoUserRelations = kataskopeoUserRelationsGraphFactory.getNoTx();
		for (String edgeClass : EDGE_CLASS_NAMES) {
			kataskopeoUserRelations.createEdgeType(edgeClass);
		}
		OrientVertexType type = kataskopeoUserRelations.createVertexType(VERTEX_CLASS_NAME);
		type.createProperty("user_id", OType.STRING);
		kataskopeoUserRelations.createKeyIndex("user_id", Vertex.class, new Parameter<String, String>("class", VERTEX_CLASS_NAME));
		kataskopeoUserRelations.commit();
	}
	
	private void populateKataskopeoUserRelations() {
		OrientGraph kataskopeo = kataskopeoGraphFactory.getTx();
		OCommandSQL query = new OCommandSQL("SELECT * FROM ID_USER LIMIT 10");
		Iterable<OrientVertex> user_ids = kataskopeo.command(query).execute();
		
		OrientGraph kataskopeoUserRelations = kataskopeoUserRelationsGraphFactory.getTx();
		for (OrientVertex user : user_ids) {
			kataskopeoUserRelations.addVertex("class:USER", "user_id", user.getProperty("value"));
		}
		kataskopeoUserRelations.commit();
	}

	private void createRelations(){
		OrientGraph kataskopeo = kataskopeoGraphFactory.getTx();
		OrientGraph kataskopeoUserRelations = kataskopeoUserRelationsGraphFactory.getTx();

		OCommandSQL query = new OCommandSQL("SELECT * FROM USER");
		Iterable<OrientVertex> user_idsFirst = kataskopeoUserRelations.command(query).execute();
		
		String idFirst, idSecond = "";
		for (OrientVertex userFirst : user_idsFirst) {
			idFirst = userFirst.getProperty("user_id");
			Iterable<OrientVertex> user_idsSecond = kataskopeoUserRelations.command(query).execute();
			for (OrientVertex userSecond : user_idsSecond) {
				idSecond = userSecond.getProperty("user_id");
				if(!idFirst.equals(idSecond)) {
					shortestPath(kataskopeo, kataskopeoUserRelations, userFirst, userSecond);
				}
			}
		}
		kataskopeoUserRelations.commit();
	}

	private void shortestPath(OrientGraph kataskopeo, OrientGraph kataskopeoUserRelations, OrientVertex userFirst, OrientVertex userSecond) {
		String idFirst = userFirst.getProperty("user_id");
		String idSecond = userSecond.getProperty("user_id");
		OCommandSQL queryCommand = new OCommandSQL(SQL_SHORTEST_PATH);
		Iterable<OrientVertex> result = kataskopeo.command(queryCommand).execute(idFirst, idSecond);
		if(result.iterator().hasNext()) {
			for (OrientVertex dio : result) {
				System.out.print(dio.getProperty("value")+" ");
				kataskopeoUserRelations.addEdge("class:Relation", userFirst, userSecond, "Relation");
			}
			System.out.println("");
		} else {
			System.out.println("DIO BONO");
		}
	}
	
}
