package tietokantasovellus;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class UI {

    final private Scanner scanner;
    final private Connection conn;
    private PreparedStatement p;
    private Statement s;
    final private DateFormat pvm;
    final private DateFormat klo;
    private boolean databaseCreated;
    final private Random rdm;

    public UI(Scanner scanner, Connection conn) {
        this.scanner = scanner;
        this.conn = conn;
        try {
            this.s = conn.createStatement();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        this.pvm = new SimpleDateFormat("dd.MM.yyyy");
        this.klo = new SimpleDateFormat("HH.mm.ss");
        this.databaseCreated = false;
        this.rdm = new Random();
    }

    //käyttöjärjestelmän aloitus
    public void start() {
        //Muuttujat, joita luetaan käyttäjältä
        String nimi;
        String paikka;
        String koodi;
        String kuvaus;
        String paiva;

        System.out.println("Tervetuloa pakettisovellukseen");
        while (true) {
            System.out.print("\nValitse toiminto (0-9): ");
            //Luetaan syöte
            String input = scanner.nextLine();
            //Komennon validointi
            if (input.equals("empty")) {
                this.emptyDatabase();
                continue;
            }
            int command;
            try {
                command = Integer.parseInt(input);
            } catch (NumberFormatException e) {
                System.out.println("Virheellinen syöte.");
                continue;
            }
            //Komennon tulkinta
            switch (command) {
                //Poistuu sovelluksesta
                case 0:
                    System.out.println("Näkemiin");
                    return;
                //Luo tietokannan taulut
                case 1:
                    this.createDatabase();
                    break;
                //Lisää käyttäjän syöttämän paikan tietokantaan
                case 2:
                    if (!(this.databaseCreated)) {
                        System.out.println("Tietokantaa ei ole luotu");
                        break;
                    }
                    System.out.print("Anna paikan nimi: ");
                    paikka = scanner.nextLine();
                    if (this.notValid(paikka)) {
                        System.out.println("Tyhjä syöte ei kelpaa");
                        break;
                    }
                    if (this.addPlace(paikka)) {
                        System.out.println("Paikka " + paikka + " lisätty");
                    } else {
                        System.out.println("Virhe paikkaa lisättäessä");
                    }
                    break;
                //Lisää käyttäjän syöttämän asiakkaan tietokantaan
                case 3:
                    if (!(this.databaseCreated)) {
                        System.out.println("Tietokantaa ei ole luotu");
                        break;
                    }
                    System.out.print("Anna asiakkaan nimi: ");
                    nimi = scanner.nextLine();
                    if (this.notValid(nimi)) {
                        System.out.println("Tyhjä syöte ei kelpaa");
                        break;
                    }
                    if (this.addCustomer(nimi)) {
                        System.out.println("Asiakas " + nimi + " lisätty");
                    } else {
                        System.out.println("Virhe asikasta lisättäessä");
                    }
                    break;
                //Lisää käyttäjän syöttämän paketin halutulle asiakkaalle
                case 4:
                    if (!(this.databaseCreated)) {
                        System.out.println("Tietokantaa ei ole luotu");
                        break;
                    }
                    System.out.print("Anna paketin koodi: ");
                    koodi = scanner.nextLine();
                    if (this.notValid(koodi)) {
                        System.out.println("Tyhjä syöte ei kelpaa");
                        break;
                    }
                    System.out.print("Anna asiakkaan nimi: ");
                    nimi = scanner.nextLine();
                    if (this.notValid(nimi)) {
                        System.out.println("Tyhjä syöte ei kelpaa");
                        break;
                    }
                    if (this.addPacket(koodi, nimi)) {
                        System.out.println("Paketti " + koodi + " lisätty asiakkaalle " + nimi);
                    } else {
                        System.out.println("Virhe pakettia lisättäessä");
                    }
                    break;
                //Lisää käyttäjän syöttämän tapahtuman halutulle paketille
                case 5:
                    if (!(this.databaseCreated)) {
                        System.out.println("Tietokantaa ei ole luotu");
                        break;
                    }
                    System.out.print("Anna paketin seurantakoodi: ");
                    koodi = scanner.nextLine();
                    if (this.notValid(koodi)) {
                        System.out.println("Tyhjä syöte ei kelpaa");
                        break;
                    }
                    System.out.print("Anna tapahtuman paikka: ");
                    paikka = scanner.nextLine();
                    if (this.notValid(paikka)) {
                        System.out.println("Tyhjä syöte ei kelpaa");
                        break;
                    }
                    System.out.print("Anna tapahtuman kuvaus: ");
                    kuvaus = scanner.nextLine();
                    if (this.notValid(kuvaus)) {
                        System.out.println("Tyhjä syöte ei kelpaa");
                        break;
                    }
                    if (this.addEvent(koodi, paikka, kuvaus)) {
                        System.out.println("Tapahtuma " + kuvaus + " lisätty paketille " + koodi + " paikassa " + paikka);
                    } else {
                        System.out.println("Virhe tapahtumaa lisättäessä");
                    }
                    break;
                //Hakee halutun paketin kaikki tapahtumat
                case 6:
                    if (!(this.databaseCreated)) {
                        System.out.println("Tietokantaa ei ole luotu");
                        break;
                    }
                    System.out.print("Anna paketin seurantakoodi: ");
                    koodi = scanner.nextLine();
                    this.searchEvents(koodi);
                    break;
                //Hakee halutun asiakkaan paketit ja pakettien tapahtumien lukumäärän
                case 7:
                    if (!(this.databaseCreated)) {
                        System.out.println("Tietokantaa ei ole luotu");
                        break;
                    }
                    System.out.print("Anna asiakkaan nimi: ");
                    nimi = scanner.nextLine();
                    this.searchPacketsAndEvents(nimi);
                    break;
                //Hakee annetun paikan tapahtumien lukumäärän tiettynä päivänä
                case 8:
                    if (!(this.databaseCreated)) {
                        System.out.println("Tietokantaa ei ole luotu");
                        break;
                    }
                    System.out.print("Anna paikan nimi: ");
                    paikka = scanner.nextLine();
                    System.out.print("Anna päivämäärä (pp.kk.vvvv): ");
                    paiva = scanner.nextLine();
                    this.searchEventsByDay(paikka, paiva);
                    break;
                //Tyhjää tietokannan ja suorittaa nopeustestin
                case 9:
                    if (!(this.databaseCreated)) {
                        System.out.println("Tietokantaa ei ole luotu");
                        break;
                    }
                    this.speedTest();
                    break;
                //Virheellinen komento ilmoitetaan
                default:
                    System.out.println("Virheellinen komento.");
            }
        }
    }

    //Komento 9
    public void speedTest() {
        this.emptyDatabase();
        this.createDatabase();
        long alku;
        long loppu;
        try {
            //Lisätään 1000 paikkaa
            alku = System.nanoTime();
            s.execute("BEGIN TRANSACTION");
            p = conn.prepareStatement("INSERT INTO Paikat(paikka) VALUES (?)");
            for (int i = 1; i <= 1000; i++) {
                p.setString(1, "P" + i);
                p.executeUpdate();
            }
            loppu = System.nanoTime();
            System.out.println("Paikkojen lisäämiseen aikaa kului " + (loppu - alku) / 1E6 + " ms");
            //Lisätään 1000 asiakasta
            alku = System.nanoTime(); 
            p = conn.prepareStatement("INSERT INTO Asiakkaat(nimi) VALUES (?)");
            for (int i = 1; i <= 1000; i++) {
                p.setString(1, "A" + i);
                p.executeUpdate();
            }
            loppu = System.nanoTime();
            System.out.println("Aiakkkaiden lisäämiseen aikaa kului " + (loppu - alku) / 1E6 + " ms");
            //Lisätään 1000 pakettia
            alku = System.nanoTime();
            p = conn.prepareStatement("INSERT INTO Paketit(asiakas_id, koodi) VALUES (?,?)");
            for (int i = 1; i <= 1000; i++) {
                int random = rdm.nextInt(1000) + 1;
                p.setInt(1, random);
                p.setString(2, "K" + i);
                p.executeUpdate();
            } 
            loppu = System.nanoTime();
            System.out.println("Pakettien lisäämiseen aikaa kului " + (loppu - alku) / 1E6 + " ms");
            //Lisätään 1000000 tapahtumaa 
            alku = System.nanoTime();
            p = conn.prepareStatement("INSERT INTO Tapahtumat(paketti_id, paikka_id, kuvaus, pvm, klo) VALUES (?,?,?,?,?)");
            for (int i = 1; i <= 1000000; i++) {
                int random1 = rdm.nextInt(1000) + 1;
                int random2 = rdm.nextInt(1000) + 1;
                p.setInt(1, random1);
                p.setInt(2, random2);
                p.setString(3, "T" + i);
                p.setString(4, pvm.format(Calendar.getInstance().getTime()));
                p.setString(5, klo.format(Calendar.getInstance().getTime()));
                p.executeUpdate();
            }
            loppu = System.nanoTime();          
            System.out.println("Tapahtumien lisäämiseen aikaa kului " + (loppu - alku) / 1E6 + " ms");
            s.execute("COMMIT");
            //Suoritetaan 1000 kyselyä, joissa haetaan jonkin asiakkaan pakettien lukumäärä
            alku = System.nanoTime();
            p = conn.prepareStatement("SELECT COUNT(id) FROM Paketit WHERE asiakas_id = ?");
            for (int i = 1; i <= 1000; i++) {
                int random = rdm.nextInt(1000) + 1;
                p.setInt(1, random);
                p.executeQuery();
            }
            loppu = System.nanoTime();
            System.out.println("Kyselyjen suorittamiseen aikaa kului " + (loppu - alku)/1E6 + " ms");
            //Suoritetaan 1000 kyselyä, joissa haetaan jonkin paketin tapahtumien lukumäärä
            alku = System.nanoTime();
            p = conn.prepareStatement("SELECT COUNT(id) FROM Tapahtumat WHERE paketti_id = ?");
            for (int i = 1; i <= 1000; i++) {
                int random = rdm.nextInt(1000) + 1;
                p.setInt(1, random);
                p.executeQuery();
            }
            loppu = System.nanoTime();
            System.out.println("Kyselyjen suorittamiseen aikaa kului " + (loppu - alku)/1E6 + " ms");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    //Komento 8
    public void searchEventsByDay(String paikka, String paiva) {
        try {
            p = conn.prepareStatement("SELECT id FROM Paikat WHERE paikka = ?");
            p.setString(1, paikka);
            int paikka_id = p.executeQuery().getInt("id");
            p = conn.prepareStatement("SELECT COUNT(id) FROM Tapahtumat WHERE (paikka_id = ? AND pvm = ?)");
            p.setInt(1, paikka_id);
            p.setString(2, paiva);
            ResultSet r = p.executeQuery();
            int maara = r.getInt(1);
            while (r.next()) {
                System.out.println(paikka + ", " + "tapahtumia " + maara);
            }
        } catch (SQLException e) {
            System.out.println("Virhe tapahtumia haettaessa");
        }
    }

    //Komento 7
    public void searchPacketsAndEvents(String nimi) {
        try {
            p = conn.prepareStatement("SELECT id FROM Asiakkaat WHERE nimi = ?");
            p.setString(1, nimi);
            int asiakas_id = p.executeQuery().getInt("id");
            p = conn.prepareStatement("SELECT id, koodi FROM Paketit WHERE asiakas_id = ?");
            p.setInt(1, asiakas_id);
            ResultSet r = p.executeQuery();
            PreparedStatement paketit;
            while (r.next()) {
                int id = r.getInt("id");
                String koodi = r.getString("koodi");
                paketit = conn.prepareStatement("SELECT COUNT(id) FROM Tapahtumat WHERE paketti_id = ?");
                paketit.setInt(1, id);
                ResultSet tapahtumat = paketit.executeQuery();
                System.out.println(koodi + ", " + "tapahtumia " + tapahtumat.getInt("COUNT(id)"));
            }
        } catch (SQLException e) {
            System.out.println("Virhe pakettien ja tapahtumien hakemisessa");
        }
    }

    //Komento 6
    public void searchEvents(String koodi) {
        try {
            p = conn.prepareStatement("SELECT id FROM Paketit WHERE koodi = ?");
            p.setString(1, koodi);
            int paketti_id = p.executeQuery().getInt("id");
            p = conn.prepareStatement("SELECT * FROM Tapahtumat WHERE paketti_id = ?");
            p.setInt(1, paketti_id);
            ResultSet r = p.executeQuery();
            p = conn.prepareStatement("SELECT paikka FROM Paikat WHERE id = ?");
            p.setInt(1, r.getInt("paikka_id"));
            String paikka = p.executeQuery().getString("paikka");
            while (r.next()) {
                System.out.println
                (paikka + ", " + r.getString("kuvaus") + " " + r.getString("pvm") + " " + "klo. " + r.getString("klo"));
            }
        } catch (SQLException e) {
            System.out.println("Virhe tapahtumien hakemisessa");
        }
    }

    //Komento 5
    public boolean addEvent(String koodi, String paikka, String kuvaus) {
        try {
            p = conn.prepareStatement("SELECT id FROM Paketit WHERE koodi = ?");
            p.setString(1, koodi);
            int paketti_id = p.executeQuery().getInt("id");
            p = conn.prepareStatement("SELECT id FROM Paikat WHERE paikka = ?");
            p.setString(1, paikka);
            int paikka_id = p.executeQuery().getInt("id");
            p = conn.prepareStatement("INSERT INTO Tapahtumat(paketti_id, paikka_id, kuvaus, pvm, klo) VALUES (?,?,?,?,?)");
            p.setInt(1, paketti_id);
            p.setInt(2, paikka_id);
            p.setString(3, kuvaus);
            p.setString(4, pvm.format(Calendar.getInstance().getTime()));
            p.setString(5, klo.format(Calendar.getInstance().getTime()));
            p.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    //Komento 4
    public boolean addPacket(String koodi, String nimi) {
        try {
            p = conn.prepareStatement("SELECT id FROM Asiakkaat WHERE nimi = ?");
            p.setString(1, nimi);
            int asiakas_id = p.executeQuery().getInt("id");
            p = conn.prepareStatement("INSERT INTO Paketit(asiakas_id, koodi) VALUES (?,?)");
            p.setInt(1, asiakas_id);
            p.setString(2, koodi);
            p.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    //Komento 3
    public boolean addCustomer(String nimi) {
        try {
            p = conn.prepareStatement("INSERT INTO Asiakkaat(nimi) VALUES (?)");
            p.setString(1, nimi);
            p.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    //Komento 2
    public boolean addPlace(String paikka) {
        try {
            p = conn.prepareStatement("INSERT INTO Paikat(paikka) VALUES (?)");
            p.setString(1, paikka);
            p.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    //Komento 1
    public void createDatabase() {
        try {
            //Luodaan tarvittavat taulut
            s.execute("CREATE TABLE IF NOT EXISTS Paikat "
                    + "(id INTEGER PRIMARY KEY, paikka TEXT UNIQUE )");
            s.execute("CREATE TABLE IF NOT EXISTS Asiakkaat "
                    + "(id INTEGER PRIMARY KEY, nimi TEXT UNIQUE)");
            s.execute("CREATE TABLE IF NOT EXISTS Paketit "
                    + "(id INTEGER PRIMARY KEY, asiakas_id INTEGER REFERENCES Asiakkaat, koodi TEXT UNIQUE)");
            s.execute("CREATE TABLE IF NOT EXISTS Tapahtumat"
                    + "(id INTEGER PRIMARY KEY, paketti_id INTEGER REFERENCES Paketit, "
                    + "paikka_id INTEGER REFERENCES Paikat, kuvaus TEXT, pvm TEXT, klo TEXT)");
            this.databaseCreated = true;
            //Halutessa luodaan indeksit tietokantaan
            s.execute("CREATE INDEX idx_as_id ON Paketit (asiakas_id)");
            s.execute("CREATE INDEX idx_pak_id ON Tapahtumat (paketti_id)");
            System.out.println("Tietokanta luotu");
        } catch (SQLException e) {
            System.out.println("Tietokanta on jo olemassa");
        }
    }
    
    //Tarkastaa, ettei syöte ole tyhjä
    public boolean notValid(String input) {
        return input.equals("");
    }
    
    //Tyhjää tietokannan
    public void emptyDatabase() {
        try {
            s.execute("DROP TABLE Tapahtumat");
            s.execute("DROP TABLE Paketit");
            s.execute("DROP TABLE Asiakkaat");
            s.execute("DROP TABLE Paikat");
            this.databaseCreated = false;
            System.out.println("Tietokanta tyhjätty");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }    
    }
}
