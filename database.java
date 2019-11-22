package filecompare;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class database {

	Connection con;
	/*
	 * String url="jdbc:postgresql://192.168.208.73:5432/gigadb_v3/"; String
	 * password="gigadb2013"; String user="gigadb";
	 */
	// set database username and password
	String url = "jdbc:postgresql://localhost:5432/gigadb_v3/";
	String password = "gigadb2013";
	String user = "gigadb_jesse";

	Statement stmt;
	PreparedStatement prepforall = null;

	public database() {

		try {
			Class.forName("org.postgresql.Driver").newInstance();
			con = DriverManager.getConnection(url, user, password);
			// this is important
			con.setAutoCommit(true);
			stmt = con.createStatement();

			// int i=1;

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void getid() throws SQLException {

		String query = "select * from dataset;";
		ResultSet resultSet = stmt.executeQuery(query);
		while (resultSet.next()) {

			System.out.println(resultSet.getInt(1));

		}
	}

	public ArrayList getdataset_manuscript() throws SQLException

	{

		String query = "select identifier from dataset where upload_status='Published' and id NOT IN (select dataset_id from manuscript, dataset where manuscript.dataset_id=dataset.id) order by identifier;";
		ResultSet rs = null;
		String location = null;
		ArrayList<String> aa = new ArrayList<String>();
		rs = stmt.executeQuery(query);
		while (rs.next()) {
			location = rs.getString(1);

			aa.add(location);

		}

		return aa;

	}

	public int getallpublished_doi() throws SQLException {
		String query = "select count(id) from dataset where upload_status='Published';";
		ResultSet rs = null;
		int number = 0;
		rs = stmt.executeQuery(query);
		while (rs.next()) {
			number = rs.getInt(1);
		}

		return number;

	}

	public String getallunpublished_doi() throws SQLException {
		String query = "select identifier, title from dataset where upload_status != 'Published' order by identifier;";
		ResultSet rs = null;
		String content = "";
		rs = stmt.executeQuery(query);
		while (rs.next()) {
			String temp = "";
			temp += rs.getString(1) + ": " + rs.getString(2);
			content += temp + "\n";
		}

		return content;

	}

	public int getdatasetid(String doi) throws SQLException {
		String query = "select id from dataset where identifier='" + doi + "';";
		ResultSet rs = null;
		int content = 0;
		rs = stmt.executeQuery(query);
		while (rs.next()) {
			content = rs.getInt(1);
		}

		return content;

	}

	public Map<Integer, String> getfiles(int datasetid) throws SQLException {
		String query = "select id,location from file where dataset_id='" + datasetid + "';";
		Map<Integer, String> databaselist = new HashMap<Integer, String>();
		ResultSet rs = null;
		rs = stmt.executeQuery(query);
		while (rs.next()) {
			databaselist.put(rs.getInt(1), rs.getString(2));
		}

		return databaselist;

	}

	public long getallpublished_size() throws SQLException {
		long number = 0;

		String query = "select SUM(CAST(dataset_size AS BIGINT)) from dataset where upload_status='Published';";
		ResultSet rs = null;
		rs = stmt.executeQuery(query);
		while (rs.next()) {
			Object value = rs.getObject(1);
			number = ((Number) value).longValue();
		}
		return number;
	}

	public void deleteoldmd5(int dataset_id) throws SQLException {
		PreparedStatement prep = null;
		String query = "delete from file_attributes where attribute_id=605 and file_id in (select id from file where dataset_id="
				+ dataset_id + ");";

		System.out.println(query);
		prep = con.prepareStatement(query);
		prep.executeUpdate();
		prep.close();

	}

	public void addfile_md5(int file_id, String value) throws SQLException {

		PreparedStatement prep1 = null;
		String query1 = "insert into file_attributes(file_id, attribute_id, value) values(?,?,?)";
		prep1 = con.prepareStatement(query1);
		prep1.setInt(1, file_id);
		prep1.setInt(2, 605);
		prep1.setString(3, value);
		prep1.executeUpdate();

	}

	public void close() throws SQLException {
		con.close();

	}

	public static void main(String[] args) throws Exception {
		database db = new database();
		// database.calPhraseProb();
		// System.out.println(database.exist("1.5524/100003"));
	}

}
