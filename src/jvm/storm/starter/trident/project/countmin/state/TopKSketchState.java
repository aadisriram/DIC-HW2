/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.trident.project.countmin.state;

import storm.trident.state.State;

import java.util.Collections;

import java.util.Comparator;
import java.util.PriorityQueue;

import storm.starter.trident.project.countmin.state.TweetWord;

import java.io.Serializable;

import java.util.*;

/**
 * Top-K counter with sliding window, where the window is defined as
 * the number of tweets, in this case it defaults to 1000.
 */
public class TopKSketchState implements State, Serializable {

    long size;
    Map<String, Long> countMap = new HashMap<String, Long>();
    public static int topk_size = 10;
    TweetWord[][] windowTweets = new TweetWord[1000][100];
    int current_count = 0;

    class WordComparator implements Comparator<TweetWord> {

        public int compare(TweetWord a, TweetWord b) {
            if(a.count > b.count)
                return 1;
            else
                return -1;
        }
    }

    PriorityQueue<TweetWord> queue = new PriorityQueue<TweetWord>(10, new WordComparator());

    TopKSketchState() {
        
    }

    public TopKSketchState(int topk_size) {
        this.topk_size = topk_size;
    }
    
    public void add(String item, long count) {

        item = item.trim();
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }

        for(TweetWord tweetTag : windowTweets[current_count]) {
            if(tweetTag == null)
                continue;

            if(countMap.containsKey(tweetTag.word)) {
                long ct = countMap.get(tweetTag.word);
                if(ct == 1)
                    countMap.remove(tweetTag.word);
                else
                    countMap.put(tweetTag.word, ct - 1);
            }

            if(queue.contains(tweetTag)) {
                queue.remove(tweetTag);

                long ct = estimateCount(tweetTag.word);
                queue.add(new TweetWord(tweetTag.word, ct));
            }
            
        }

        int t_count = 0;
        for(String hashtag : item.split(" ")) {
            windowTweets[current_count] = new TweetWord[100];
            windowTweets[current_count][t_count++] = new TweetWord(hashtag, 1L);
 
            if(countMap.containsKey(hashtag)) {
                countMap.put(hashtag, countMap.get(hashtag) + count);
            } else {
                countMap.put(hashtag, count);
            }
            size += count;
            //Maintaining the top K items in the PQueue
            if(item.length() > 0) {

                //Estimate the count
                long ct = estimateCount(item);
                TweetWord tw = new TweetWord(item, ct);
                tw.word = item;
            
                //If the PQueue already has the element, remove it
                if(queue.contains(tw)) {
                    queue.remove(tw);
                }

                //Add the element to the PQueue
                queue.add(tw);

                //Remove all elements apart from the top K
                while(queue.size() > topk_size)
                    queue.poll();
            }
        }
        current_count = (current_count + 1)%1000;
    }

    
    public long size() {
        return size;
    }
    
    public long estimateCount(String item) {

        if(countMap.containsKey(item)) {
            return countMap.get(item);
        }

        return 0;
    }

    @Override
    public void beginCommit(Long txid) {
        return;
    }

    @Override
    public void commit(Long txid) {
        return;
    }

    @SuppressWarnings("serial")
    protected static class CMSMergeException extends RuntimeException {
   // protected static class CMSMergeException extends FrequencyMergeException {

        public CMSMergeException(String message) {
            super(message);
        }
    }
}
