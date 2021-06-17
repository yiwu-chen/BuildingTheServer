import java.util.Arrays;
import java.util.Map;

/**
 * The word count function that able to track the count of each word in a hashmap.
 */
public class WordCount {
  /**
   * The word count method that count the number of unique words in a line.
   *
   * @param wordsMap the hashmap that stores the tuple for every word in the lines.
   * @param text     the line
   */
  public static void wordCount(Map<String, Integer> wordsMap, String text) {
    Arrays.asList(text.split("\\s+")).forEach(s -> {
      if (wordsMap.containsKey(s)) {
        Integer count = wordsMap.get(s);
        wordsMap.put(s, count + 1);
      } else {
        wordsMap.put(s, 1);
      }
    });
  }
}
