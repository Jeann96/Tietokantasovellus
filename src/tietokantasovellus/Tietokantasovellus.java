package tietokantasovellus;
import java.sql.*;
import java.util.*;

public class Tietokantasovellus {

    public static void main(String[] args) {
        Connection conn = getConnection();
        if (conn == null) {
            System.out.println("Virhe yhtyden muodostamisessa.");
            return;
        }
        Scanner scanner = new Scanner(System.in);
        new UI(scanner, conn).start();
    }
    
    public static Connection getConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:testi.db");
        } catch (SQLException e) {
            System.out.println("Virhe yhteyttä muodostaessa");
        }
        return conn;
    }
}
