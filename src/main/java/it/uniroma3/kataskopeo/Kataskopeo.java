package it.uniroma3.kataskopeo;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

import it.uniroma3.user_relations.UserRelations;

public class Kataskopeo {

	private static boolean  INIT_KATASKOPEO = false; 
	private static String   MYSQL_KATASKOPEO_URL = "jdbc:mysql://localhost:3306/Kataskopeo_hash?serverTimezone=UTC&autoReconnect=true&useSSL=false";
	private static String   ORIENTDB_KATASKOPEO_URL = "remote:localhost/database/Kataskopeo";
	private static String   ORIENTDB_KATASKOPEO_USER_RELATIONS_URL = "remote:localhost/database/KataskopeoUserRelations";
	private static String[] TABLE_CLASS_NAMES = { "AnagraficaDealer", "CanaleVendita", "CFMSAtt", "CRMC", "DettaglioRDS", "Device", "IMEINetworking", "OrdinativiFisso", "TestataDWI" }; 
	private static String[] SECONDARY_VERTEX_CLASSES = { 
			//   0                 1               2         3       4        5           6         7              8 
			"CODICE_FISCALE", "PARTITA_IVA", "INDIRIZZO", "CITTA", "CAP", "PROVINCIA", "DSLOC", "TELEFONO", "CANALE_VENDITA",
			// 9             10                        11             12      13           14                      15
			"LINEA", "CODICE_PARTNER_VENDITA", "MARCAGGIO_CLIENTE", "DATA", "STATO", "RAGIONE_SOCIALE", "CODICE_ANAGRAFICA_DEALER",
			//      16                   17                 18      19             20                           21
			"NUMERO_DOCUMENTO", "CODICE_IMEI_NETWORKING", "IMEI", "IMSI", "VISITOR_LOCATION_REGISTER", "CODICE_LOCATION_AREA",
			//  22	          23             24                   25                  26                27           28
			"ID_UER", "TIPO_CLIENTE", "TIPO_SERVIZIO_CRM", "CODICE_SERVIZIO", "CODICE_SEGMENTO", "NUMERO_ORDINE", "ID_USER",
			//      29                     30                      31                  32              33                   34
			"COD_ACLI_CLIENTE", "NUM_CONTRATTO_CLIENTE", "COD_TIPOLOGIA_ORDINE", "OWNER_ORDINE", "CODICE_DEALER", "CODICE_PUNTO_VENDITA"
	};

	public static void main(String[] args) {
		String mysql_username = args[0];
		String mysql_password = args[1];
		String orientdb_username = args[2];
		String orientdb_password = args[3];

		OrientGraphFactory kataskopeoGraphFactory;
		if(INIT_KATASKOPEO){
			System.out.println("Connecting mysql database...");
			Connection mysqlConnection = null;
			try {
				mysqlConnection = DriverManager.getConnection(MYSQL_KATASKOPEO_URL, mysql_username, mysql_password);
				System.out.println("Mysql database is connected!");
				kataskopeoGraphFactory = initOrientDB(orientdb_username, orientdb_password, ORIENTDB_KATASKOPEO_URL);
				tablesFactory(mysqlConnection, kataskopeoGraphFactory);
				mysqlConnection.close();
			} catch (SQLException e) {
				throw new IllegalStateException("Cannot connect at mysql database!", e);
			}
		} else {
			System.out.println("Connecting Kataskopeo database...");
			kataskopeoGraphFactory = new OrientGraphFactory(ORIENTDB_KATASKOPEO_URL, orientdb_username, orientdb_password).setupPool(1,10);
			System.out.println("Kataskopeo database is connected!");
			OrientGraphFactory kataskopeoUserRelationsGraphFactory = initOrientDB(orientdb_username, orientdb_password, ORIENTDB_KATASKOPEO_USER_RELATIONS_URL);
			new UserRelations(kataskopeoGraphFactory, kataskopeoUserRelationsGraphFactory);
		}
	}

	private static void tablesFactory(Connection mysqlConnection, OrientGraphFactory orientDbFactory) {
		Class<?> c;
		Constructor<?> cons;

		System.out.println("Creating Class...");
		createClasses(orientDbFactory);
		System.out.println("Done");
		for (String tableClass : TABLE_CLASS_NAMES) {
			System.out.println("Processing "+tableClass+"...");
			try {
				c = Class.forName("it.uniroma3.tables."+tableClass);
				cons = c.getConstructor(Connection.class, OrientGraphFactory.class, String[].class);
				cons.newInstance(mysqlConnection, orientDbFactory, SECONDARY_VERTEX_CLASSES);
			} catch (Exception e) {				
				e.printStackTrace();
			}
			System.out.println("Done.");
		}
	}

	private static OrientGraphFactory initOrientDB(String orientdb_username, String orientdb_password, String orientdb_url ){
		OrientGraphFactory orientDbFactory = null;
		OServerAdmin serverAdmin;
		// drop Kataskopeo database if exists
		try {
			serverAdmin = new OServerAdmin(orientdb_url).connect(orientdb_username, orientdb_password);
			serverAdmin.dropDatabase("Kataskopeo");
		} catch (Exception e) {} 
		// create a new Kataskopeo database
		try {
			serverAdmin = new OServerAdmin(orientdb_url).connect(orientdb_username, orientdb_password);
			serverAdmin.createDatabase("graph", "plocal");
			orientDbFactory = new OrientGraphFactory(orientdb_url, orientdb_username, orientdb_password).setupPool(1,10);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return orientDbFactory;
	}

	private static void createClasses(OrientGraphFactory orientDbFactory) {
		OrientGraphNoTx graph = orientDbFactory.getNoTx();
		String className = "";
		for (int i = 0; i < SECONDARY_VERTEX_CLASSES.length; i++) {
			className = SECONDARY_VERTEX_CLASSES[i];
			createVertexClass(graph, className);
		}
		graph.commit();
	}
	private static void createVertexClass(OrientGraphNoTx graph, String className){
		OrientVertexType type = graph.createVertexType(className);
		type.createProperty("value", OType.STRING);
		graph.createKeyIndex("value", Vertex.class, new Parameter<String, String>("class", className));
	}

}
