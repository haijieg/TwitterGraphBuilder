package graphbuilder.twitter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import cmu.arktweetnlp.Twokenize;

public class TestTwokenizer {
	public static void main (String[] args) throws FileNotFoundException, IOException {
		/*
		File inputpath = new File(args[0]);
		int parsedcount = 0;
		for (File file : inputpath.listFiles()) {
			System.out.println("Parsing " + file.getName());
			InputStream in = new GZIPInputStream(new FileInputStream(file));
			Reader decoder = new InputStreamReader(in);
			BufferedReader buffered = new BufferedReader(decoder);
			String line = null;
			while((line = buffered.readLine()) != null) {
				try {
					TweetsJSParser parser = new TweetsJSParser(line);
					parser.tokenize();
					parsedcount++;
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (parsedcount % 10000 == 0)
					System.out.println("Parsed " + parsedcount + "Tweets");
			}
		}
		*/
		
		String test = ".........   ..................." +
				"   ............................................................";
		// test = test.toLowerCase().replaceAll("\\.{3,}", "\\.");
		System.out.println(test);
		List<String> ls = Twokenize.tokenize(test);
		System.out.println(ls.size());
		System.out.println("done");		
	}
}
