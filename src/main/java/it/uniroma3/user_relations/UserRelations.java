package it.uniroma3.user_relations;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class UserRelations {

	private static OrientGraph kataskopeo;
	private static OrientGraph kataskopeoUserRelations;

	public UserRelations(OrientGraphFactory kataskopeoGraphFactory, OrientGraphFactory kataskopeoUserRelationsGraphFactory) {
		this.kataskopeo = kataskopeoGraphFactory.getTx();
		this.kataskopeoUserRelations = kataskopeoUserRelationsGraphFactory.getTx();
		initKataskopeoUserRelations();
		path();
	} 

	private void initKataskopeoUserRelations() {
		String className = "USER";
		OrientVertexType type = kataskopeoUserRelations.createVertexType(className);
		type.createProperty("user_id", OType.STRING);
		kataskopeoUserRelations.createKeyIndex("user_id", Vertex.class, new Parameter<String, String>("class", className));
		kataskopeoUserRelations.commit();
		OCommandSQL query = new OCommandSQL("SELECT * FROM ID_USER");
		Iterable<OrientVertex> user_ids = kataskopeo.command(query).execute();
		for (OrientVertex user : user_ids) {
			kataskopeoUserRelations.addVertex("class:USER", "user_id", user.getProperty("value"));
		}
		kataskopeoUserRelations.commit();
	}

	private void path(){
//		OCommandSQL query = new OCommandSQL("SELECT $path FROM ( TRAVERSE out('Has_CODICE_TESTATA_DWI'), out('Has_NUM_DOCUMENTO_CLIENTE'), out('Has_CODICE_TESTATA_DWI'), out('Has_ID_CLIENTE') FROM #70:0 ) WHERE @class = 'ID_USER' and @rid <> #70:0");
		OCommandSQL query = new OCommandSQL("SELECT $path FROM ( TRAVERSE out('Has_CODICE_TESTATA_DWI'), out('Has_NUM_DOCUMENTO_CLIENTE'), out('Has_CODICE_TESTATA_DWI'), out('Has_ID_CLIENTE') FROM (select @rid from ID_USER WHERE VALUE = :user_id) ) WHERE @class = 'ID_USER'");
		Map<String,Object> params = new HashMap<String,Object>();
		params.put(":user_id", "7-6HL1EFD");
		Iterable<OrientVertex> ciao = kataskopeo.command(query).execute(params);
		for (OrientVertex dio : ciao) {

			System.out.println(dio.getProperty("$path"));

		}
		System.out.println("goal");
	}


}
