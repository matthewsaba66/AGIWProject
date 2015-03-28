package WarcParser;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;



public class MainClass {
	public static void main (String[] args) throws IOException, ParseException{

		IndexWriter writer = WarcParse.mkWriter();

		/*fase query*/

		/* set the maximum number of results */
		int maxHits = 10;

		/* open a directory reader and create searcher and topdocs */
		IndexReader reader1 = DirectoryReader.open(writer.getDirectory());
		IndexSearcher searcher = new IndexSearcher(reader1);
		TopScoreDocCollector collector =
				TopScoreDocCollector.create(maxHits);

		/* create the query parser */
		QueryParser qp = new QueryParser("payload", writer.getConfig().getAnalyzer());

		/* query string */
		String querystring = "specialist";
		Query q = qp.parse(querystring);

		/* search into the index */
		searcher.search(q, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		/* print results */
		System.out.println("Found " + hits.length + " hits.");
		for(int i=0;i<hits.length;++i) {
			int docId = hits[i].doc;
			Document d = searcher.doc(docId);
			System.out.println("url: " + d.get("url \n") + " body: " + d.get("payload"));
			System.out.println("-------------------------------------------\n\n\n\n");
		}


	}

}
