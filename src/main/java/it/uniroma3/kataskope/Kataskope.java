package it.uniroma3.kataskope;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import it.uniroma3.tables.AnagraficaDealer;

public class Kataskope {

	private static String MYSQL_URL = "jdbc:mysql://localhost:3306/Kataskopeo_hash?serverTimezone=UTC&autoReconnect=true&useSSL=false";
	private static String ORIENTDB_URL = "remote:localhost/database/Kataskopeo";

	public static void main(String[] args) {
		String mysql_username = args[0];
		String mysql_password = args[1];
		String orientdb_username = args[2];
		String orientdb_password = args[3];

		System.out.println("Connecting database...");
		Connection mysqlConnection = null;
		try {
			mysqlConnection = DriverManager.getConnection(MYSQL_URL, mysql_username, mysql_password);
			System.out.println("Database connected!");
			OrientGraphFactory orientDbFactory = initOrientDB(orientdb_username, orientdb_password);
			tablesFactory(mysqlConnection, orientDbFactory);
			mysqlConnection.close();
		} catch (SQLException e) {
			throw new IllegalStateException("Cannot connect the database!", e);
		}
	}

	private static void tablesFactory(Connection mysqlConnection, OrientGraphFactory orientDbFactory) {
		new AnagraficaDealer(mysqlConnection, orientDbFactory);
	}
	
	private static OrientGraphFactory initOrientDB(String orientdb_username, String orientdb_password){
		OServerAdmin serverAdmin;
		OrientGraphFactory orientDbFactory = null;
		try {
			serverAdmin = new OServerAdmin("remote:localhost/Kataskopeo").connect(orientdb_username, orientdb_password);
			serverAdmin.dropDatabase("Kataskopeo");
		} catch (Exception e) {
		} finally {
			try {
				serverAdmin = new OServerAdmin("remote:localhost/Kataskopeo").connect(orientdb_username, orientdb_password);
				serverAdmin.createDatabase("graph", "plocal");
				orientDbFactory = new OrientGraphFactory(ORIENTDB_URL, orientdb_username, orientdb_password).setupPool(1,10);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return orientDbFactory;
	}

}
