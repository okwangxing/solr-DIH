import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.collect.Lists;
public class FileImporter {
	static SolrServer server = null;

	static int counter = 0;

	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage:xxx.jar path total");
			return;
		}
		Collection<File> files = FileUtils.listFiles(new File(args[0]), null, false);
		int total = Integer.valueOf(args[1]);
		List<Review> rs = Lists.newArrayList();
		while (counter < total) {
			for (File f : files) {
				if (f.getName().startsWith("t_review_all_field.")) {
					List<String> lines = FileUtils.readLines(f, "gbk");
					String strlist = "";
					for (String line : lines) {
						int end = line.indexOf(Review.Split.SPLIT_LINE_END);
						if (end == -1) {
							strlist += line;
							continue;
						} else if ((strlist.length()) > 0 && end >= 0) {
							strlist += line;
						} else {
							strlist = line;
						}

						Review review = Review.convStr2Review(strlist);

						if (review != null) {
							rs.add(review);
						}
						strlist = "";

					}
					if (counter >= total) {
						break;
					}
					commit(rs);
					rs.clear();
				}
			}
		}
		commit(rs);
	}

	private static boolean commit(List<Review> rs) throws Exception {
		if (server == null) {
			server = getServer();
		}
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		for (Review r : rs) {
			SolrInputDocument doc1 = new SolrInputDocument();

			for (Field f : r.getClass().getDeclaredFields()) {
				if (!f.getName().equals("content") && !f.getName().equals("title")) {
					doc1.addField(f.getName(), f.getLong(r), 1.0f);
				} else {
					doc1.addField(f.getName(), f.get(r), 1.0f);
				}
			}

			docs.add(doc1);
			if (counter++ % 10000 == 0) {
				System.out.println("Process " + counter);
			}
		}
		server.add(docs);
		// 提交
		server.commit();
		System.out.println("Commit @ " + counter);
		return true;

	}

	private static SolrServer getServer() {
		String url = "http://10.12.194.136:8080/solr/reviews";
		SolrServer server = new HttpSolrServer(url);
		return server;
	}
}
