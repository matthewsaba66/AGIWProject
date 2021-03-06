package WarcParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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
import org.apache.lucene.util.packed.PackedInts.Writer;
import org.jsoup.Jsoup;
import org.jwat.common.Diagnosis;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;


public class WarcParse {



	public static IndexWriter mkWriter() throws IOException, ParseException {
		return null;

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