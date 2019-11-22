package filecompare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.io.FileUtils;

import filecompare.database;

public class main {

	public static void main(String[] args) throws SQLException, IOException, InterruptedException {
		database db = new database();
		Scanner scanner = new Scanner(System.in);
		System.out.print("Enter doi: ");
		String doi = scanner.next();
		int id = db.getdatasetid(doi);

		String file_path = "/Users/xiaosizhe/Desktop/"; // change file path

		Map<String, String> allfiles = new HashMap<String, String>();
		ArrayList<String> ftplist = new ArrayList<String>();
		URL url = new URL("ftp://parrot.genomics.cn/gigadb/pub/10.5524/100001_101000/" + doi + "/" + doi + ".md5");
		File md5sum = new File(file_path + doi + ".md5");
		FileUtils.copyURLToFile(url, md5sum);

		try (BufferedReader br = new BufferedReader(new FileReader(md5sum))) {
			String line;

			while ((line = br.readLine()) != null) {
				String[] aa = line.split("\\s+");
				aa[1] = aa[1].replace("./", "ftp://parrot.genomics.cn/gigadb/pub/10.5524/100001_101000/" + doi + "/");
				// aa[1] = aa[1].replace("./",
				// "ftp://parrot.genomics.cn/gigadb/pub/10.5524/101001_102000/"+doi+"/");
				// aa[1] = aa[1].replace("./",
				// "ftp://parrot.genomics.cn/gigadb/pub/10.5524/102001_103000/"+doi+"/");
				if (aa[1].endsWith(doi + ".md5")) {
					continue;
				}
				ftplist.add(aa[1]);
				allfiles.put(aa[1], aa[0]);
			}
		}

		Map<Integer, String> databaselist = db.getfiles(id);
		db.deleteoldmd5(id);
		checkdatabase(ftplist, databaselist, allfiles, db);
		checkftp(ftplist, databaselist);

		db.close();

	}

	public static void checkdatabase(ArrayList<String> ftplist, Map<Integer, String> databaselist,
			Map<String, String> md5, database db) throws SQLException {
		for (String test : ftplist) {
			test = test.trim();
			boolean flag = true;

			for (Map.Entry<Integer, String> dbfile : databaselist.entrySet()) {
				String filename = dbfile.getValue();
				filename = filename.trim();
				if (filename.equals(test) || filename == test) {
					flag = false;
					// System.out.println("find file: "+ test);
					// System.out.println("insert md5 value: "+md5.get(test));
					// System.out.println("file_id: "+dbfile.getKey());
					db.addfile_md5(dbfile.getKey(), md5.get(test));

				}
			}
			if (flag) {
				System.out.println("Can't find " + test + " in database");
			}

		}

	}

	public static void checkftp(ArrayList<String> ftplist, Map<Integer, String> databaselist) {

		for (Map.Entry<Integer, String> dbfile : databaselist.entrySet()) {
			String test = dbfile.getValue().trim();
			boolean flag = true;

			for (String ftfile : ftplist) {
				ftfile = ftfile.trim();
				if (ftfile.equals(test) || ftfile == test) {
					flag = false;
				}
			}
			if (flag) {
				System.out.println("Can't find " + test + " in ftpserver");
			}

		}

	}

}
