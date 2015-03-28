package WarcParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;



public class MainClass {
	public static void main (String[] args) throws IOException, ParseException{

		/* create a standard analyzer */
		StandardAnalyzer analyzer = new StandardAnalyzer( CharArraySet.EMPTY_SET);

		/* create the index in the pathToFolder or in RAM (choose one) */
		//File file = new File("pathToFolder");
		//Path path = Paths.get("/home/matteo/test/");
		//Directory index = FSDirectory.open(path);
		Directory index =new RAMDirectory();


		/* set an index config */
		IndexWriterConfig config = new IndexWriterConfig( analyzer);
		config.setOpenMode(OpenMode.CREATE);
		/* create the writer */
		IndexWriter writer = new IndexWriter(index, config);

		File file = new File("/home/matteo/CRAWL/ClueWeb/00.warc");
		
			InputStream in = new FileInputStream( file );

			int records = 0;
			int errors = 0;

			WarcReader reader = WarcReaderFactory.getReader( in );
			WarcRecord record;
			int j = 0;
			while ( (record = reader.getNextRecord()) != null && j!= 500 ) {

				++records;

				if (record.diagnostics.hasErrors()) {
					Document d = printRecord(record);
					writer.addDocument(d);
					j++;
					//printRecordErrors(record);
					//errors += record.diagnostics.getErrors().size();
				}
			}

			System.out.println("--------------");
			System.out.println("       Records: " + records);
			//System.out.println("        Errors: " + errors);
			reader.close();
			in.close();
			writer.close();


			/*fase query*/

			/* set the maximum number of results */
			int maxHits = 10;

			/* open a directory reader and create searcher and topdocs */
			IndexReader reader1 = DirectoryReader.open(writer.getDirectory());
			IndexSearcher searcher = new IndexSearcher(reader1);
			TopScoreDocCollector collector =
					TopScoreDocCollector.create(maxHits);

			/* create the query parser */

			QueryParser qp = new QueryParser("payload", analyzer);
			

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

		private static Document printRecord(WarcRecord record) throws IOException {
			// TODO Auto-generated method stub
			Document doc = new Document();
			doc.add(new StringField("type", record.header.contentTypeStr, Field.Store.YES));
			if(record.header.warcTargetUriStr!=null){
				doc.add(new StringField("url", record.header.warcTargetUriStr, Field.Store.YES));
				//System.out.println("URL: ---"+record.header.warcTargetUriStr);
			}
			doc.add(new TextField("payload",getPayload(record), Field.Store.YES));

			//System.out.println("TYPE: ---"+record.header.contentTypeStr);
			return doc;



		}

		private static String getPayload(WarcRecord record) throws IOException {
			// TODO Auto-generated method stub
			BufferedReader payin = new BufferedReader(new InputStreamReader(record.getPayload().getInputStream()));
			String line = "";
			String nextLine;
			while((nextLine = payin.readLine()) != null) {
				line = line.concat(nextLine + "\n");
			}

			//System.out.println("LINE: ---"+line);

			//org.jsoup.nodes.Document doc = Jsoup.parse(line);
			//doc.toString();

			return line;
		}

	
}



