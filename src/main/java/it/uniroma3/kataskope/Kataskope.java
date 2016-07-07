package it.uniroma3.kataskope;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import it.uniroma3.tables.AnagraficaDealer;

public class Kataskope {

	private static String MYSQL_URL = "jdbc:mysql://localhost:3306/Kataskopeo_hash?serverTimezone=UTC";
	private static String ORIENTDB_URL = "remote:localhost/database/Kataskope";

	public static void main(String[] args) {
		String username = args[0];
		String password = args[1];

		System.out.println("Connecting database...");
		Connection mysqlConnection = null;
		try {
			mysqlConnection = DriverManager.getConnection(MYSQL_URL, username, password);
			System.out.println("Database connected!");
			OrientGraphFactory orientDbFactory = new OrientGraphFactory(ORIENTDB_URL, args[2], args[3]).setupPool(1,10);
			tablesFactory(mysqlConnection, orientDbFactory);
		} catch (SQLException e) {
			throw new IllegalStateException("Cannot connect the database!", e);
		} finally {
			try {
				mysqlConnection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static void tablesFactory(Connection mysqlConnection, OrientGraphFactory orientDbFactory) {
		new AnagraficaDealer(mysqlConnection, orientDbFactory);
	}

}
