import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;

public class ParseDatabaseToTFMap implements Function<String, Map<String, Double>> {

	/**
	 * Random ID
	 */
	private static final long serialVersionUID = 1L;

	public Map<String, Double> call(String music) throws Exception {
		Pattern patternWord = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
		
		List<String> holder = new ArrayList<String>();
		Pattern pattern = Pattern.compile("\"([^\"]*)\"");
		Matcher matcher = pattern.matcher(music);
		
		while (matcher.find()) {
			String musicFactor = matcher.group(1).trim();
			if (musicFactor.isEmpty() || musicFactor.equals(" ")) {
				continue;
			}
			String[] words = patternWord.split(musicFactor);
			for (int i = 0; i < words.length; i++) {
				holder.add(words[i]);
			}			
		}
		
		Map<String, Double> mapWords = new HashMap<String, Double>();		
		for (int i = 1; i < holder.size(); i++) {
			String word = holder.get(i);
			if (mapWords.containsKey(word)) {
				mapWords.put(word, mapWords.get(word) + 1);
			}
			else {
				mapWords.put(word, (double) 1);
			}
		}
		
		for (String key : mapWords.keySet()) {
			mapWords.put(key, mapWords.get(key) / (holder.size() - 1));
		}
		
		return mapWords;
	}
	
}
