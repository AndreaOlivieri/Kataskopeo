package it.uniroma3.kataskope;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.orientechnologies.orient.client.remote.OServerAdmin;
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
		System.out.println("Processing Anagrafica Dealer...");
		new AnagraficaDealer(mysqlConnection, orientDbFactory);
		System.out.println("Done.");
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

}
