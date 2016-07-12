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

public class Kataskopeo {

	private static String MYSQL_URL = "jdbc:mysql://localhost:3306/Kataskopeo_hash?serverTimezone=UTC&autoReconnect=true&useSSL=false";
	private static String ORIENTDB_URL = "remote:localhost/database/Kataskopeo";
	private static String[] tableClassNames = { "AnagraficaDealer", "CanaleVendita", "CFMSAtt", "CRMC", "IMEINetworking", "OrdinativiFisso", "TestataDWI" }; 
	private static String[] secondaryVertexClasses = { 
			//   0                 1               2         3       4        5           6         7              8 
			"CODICE_FISCALE", "PARTITA_IVA", "INDIRIZZO", "CITTA", "CAP", "PROVINCIA", "DSLOC", "TELEFONO", "CANALE_VENDITA",
			// 9             10                        11             12      13           14                      15
			"LINEA", "CODICE_PARTNER_VENDITA", "MARCAGGIO_CLIENTE", "DATA", "STATO", "RAGIONE_SOCIALE", "CODICE_ANAGRAFICA_DEALER",
			//      16                   17                 18      19             20                           21
			"NUMERO_DOCUMENTO", "CODICE_IMEI_NETWORKING", "IMEI", "IMSI", "VISITOR_LOCATION_REGISTER", "CODICE_LOCATION_AREA",
			//  22	          23             24                   25                  26                27           28
			"ID_UER", "TIPO_CLIENTE", "TIPO_SERVIZIO_CRM", "CODICE_SERVIZIO", "CODICE_SEGMENTO", "NUMERO_ORDINE", "ID_USER",
			//      29                     30                      31                  32
			"COD_ACLI_CLIENTE", "NUM_CONTRATTO_CLIENTE", "COD_TIPOLOGIA_ORDINE", "OWNER_ORDINE", 
	};

	public static void main(String[] args) {
		String mysql_username = args[0];
		String mysql_password = args[1];
		String orientdb_username = args[2];
		String orientdb_password = args[3];

		System.out.println("Connecting mysql database...");
		Connection mysqlConnection = null;
		try {
			mysqlConnection = DriverManager.getConnection(MYSQL_URL, mysql_username, mysql_password);
			System.out.println("Mysql database is connected!");
			OrientGraphFactory orientDbFactory = initOrientDB(orientdb_username, orientdb_password);
			tablesFactory(mysqlConnection, orientDbFactory);
			mysqlConnection.close();
		} catch (SQLException e) {
			throw new IllegalStateException("Cannot connect at mysql database!", e);
		}
	}

	private static void tablesFactory(Connection mysqlConnection, OrientGraphFactory orientDbFactory) {
		Class<?> c;
		Constructor<?> cons;
		
		System.out.println("Creating Class...");
		createClasses(orientDbFactory);
		System.out.println("Done");
		for (String tableClass : tableClassNames) {
			try {
				c = Class.forName("it.uniroma3.tables."+tableClass);
				cons = c.getConstructor(Connection.class, OrientGraphFactory.class, String[].class);
				cons.newInstance(mysqlConnection, orientDbFactory, secondaryVertexClasses);
			} catch (Exception e) {				
				e.printStackTrace();
			}
		}
	}

	private static OrientGraphFactory initOrientDB(String orientdb_username, String orientdb_password){
		OServerAdmin serverAdmin;
		OrientGraphFactory orientDbFactory = null;
		// drop Kataskopeo database if exists
		try {
			serverAdmin = new OServerAdmin(ORIENTDB_URL).connect(orientdb_username, orientdb_password);
			serverAdmin.dropDatabase("Kataskopeo");
			System.out.println("Old version of Kataskopeo database has been dropped");
		} catch (Exception e) {} 
		// create a new Kataskopeo database
		try {
			serverAdmin = new OServerAdmin(ORIENTDB_URL).connect(orientdb_username, orientdb_password);
			serverAdmin.createDatabase("graph", "plocal");
			orientDbFactory = new OrientGraphFactory(ORIENTDB_URL, orientdb_username, orientdb_password).setupPool(1,10);
			System.out.println("New version of Kataskopeo database has been created");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return orientDbFactory;
	}

	private static void createClasses(OrientGraphFactory orientDbFactory) {
		OrientGraphNoTx graph = orientDbFactory.getNoTx();
		String className = "";
		for (int i = 0; i < secondaryVertexClasses.length; i++) {
			className = secondaryVertexClasses[i];
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
