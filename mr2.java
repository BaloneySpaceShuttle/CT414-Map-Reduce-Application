//Liam Thaker - 14494722

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class mr2 {

    public static void main(String[] args) {
        long startTime = System.nanoTime();
        List<File> fList = new LinkedList<File>();
        Map<String, String> input = new ConcurrentHashMap<String, String>();

        //turns input into single file

        int fileNum = 10;
        for(int i=0;i<fileNum;i++) {
            String name = "files/BigTextFile_" + (i+1) + ".txt";
            File f = new File(name);
            fList.add(f);
        }

        String combinedContent = ""; //total content of all files

        for (File file : fList) {
            String content = "";
            String newLine = System.getProperty("line.separator");
            Scanner read = null;
            try {
                read = new Scanner(file);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            read.useDelimiter(newLine);
            while (read.hasNext()) {
                content += read.next();
            }
            read.close();

            combinedContent += content;
        }

        combinedContent = combinedContent.replaceAll("[\\.$|,|;|.|!]", "");

        input.put("BigJoinedTextFile.txt", combinedContent);

        // Distributed MapReduce
        {
            //number is thread count (ex.10) thread pool
            final Map<String, Map<String, Integer>> output = new ConcurrentHashMap<String, Map<String, Integer>>();
            int numThreads = fileNum;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            //map stage

            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            List<Thread> mapCluster = new ArrayList<Thread>(input.size());

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while (inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();
                executor.execute(() -> map(file, contents, mapCallback));
            }

            executor.shutdown();

            while (!executor.isTerminated())
                ;

            //group them
            Map<String, List<String>> groupedItems = new ConcurrentHashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while (mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            //reduce stage
            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            executor = Executors.newFixedThreadPool(numThreads);

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while (groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                executor.execute(() -> reduce(word, list, reduceCallback));
            }

            executor.shutdown();
            while (!executor.isTerminated())
                ;

            long endTime = System.nanoTime();
            long duration = (endTime - startTime);

            System.out.println(output);

            System.out.println("\n" + (double) duration / 1000000000.0);
        }
    }

    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for (String word : words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new ConcurrentHashMap<String, Integer>();
        for (String file : list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for (String word : words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K, V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new ConcurrentHashMap<String, Integer>();
        for (String file : list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
}
