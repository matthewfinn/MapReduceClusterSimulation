package mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce {

	public static void main(String[] args) {

		MapReduce map = new MapReduce();

		int threads = Integer.parseInt(args[0]); //maximum number of threads allowed......doesn't have to be cmd param necessarily.

		String fname1 = args[1]; File f1 = new File(fname1);
		String fname2 = args[2]; File f2 = new File(fname2);
		String fname3 = args[3]; File f3 = new File(fname3);
		String fname4 = args[4]; File f4 = new File(fname4);
		String fname5 = args[5]; File f5 = new File(fname5);

		System.out.println(f1.getName());
		System.out.println(f2.getName());
		System.out.println(f3.getName());
		System.out.println(f4.getName());
		System.out.println(f5.getName());


		ExecutorService exe = Executors.newFixedThreadPool(threads);

		HashMap<String, String> files = new HashMap<String,String>();
		files.put(f1.getName(), map.readFile(f1));
		files.put(f2.getName(), map.readFile(f2));
		files.put(f3.getName(), map.readFile(f3));
		files.put(f4.getName(), map.readFile(f4));
		files.put(f5.getName(), map.readFile(f5));

		// APPROACH #3: Distributed MapReduce
		{
			final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

			// MAP:

			final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

			final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
				@Override
				public synchronized void mapDone(String file, List<MappedItem> results) {
					mappedItems.addAll(results);
				}
			};

			List<Thread> mapCluster = new ArrayList<Thread>(files.size());

			Iterator<Map.Entry<String, String>> inputIter = files.entrySet().iterator();
			while(inputIter.hasNext()) {
				Map.Entry<String, String> entry = inputIter.next();
				final String file = entry.getKey();
				final String contents = entry.getValue();

				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						map(file, contents, mapCallback);
					}
				});
				mapCluster.add(t);
			}

			// execute threads
			for(Thread t : mapCluster) {
				exe.execute(t);
			}
			exe.shutdown(); //kill executor
			while (!exe.isTerminated());


			// GROUP:

			Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

			Iterator<MappedItem> mappedIter = mappedItems.iterator();
			while(mappedIter.hasNext()) {
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

			// REDUCE:

			exe = Executors.newFixedThreadPool(threads);

			final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
				@Override
				public synchronized void reduceDone(String k, Map<String, Integer> v) {
					output.put(k, v);
				}
			};

			List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

			Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
			while(groupedIter.hasNext()) {
				Map.Entry<String, List<String>> entry = groupedIter.next();
				final String word = entry.getKey();
				final List<String> list = entry.getValue();

				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						reduce(word, list, reduceCallback);
					}
				});
				reduceCluster.add(t);
			}

			// wait for reducing phase to be over: Being run on threads
			for(Thread t : reduceCluster) {
				exe.execute(t);
			}

			exe.shutdown();
			while (!exe.isTerminated());

			System.out.println("Character Count Being Run On " + threads + " Threads.");
			System.out.println(output);
		}
	}

	//reads first letter of each word in the file
	public String readFile(File file) {

		StringBuilder sb = new StringBuilder();
		Scanner scanner = null;
		try {
			scanner = new Scanner(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (scanner.hasNext()) {
			String s = scanner.next().replaceAll("[^a-zA-Z]+"," ").toLowerCase();;
			sb.append(s.substring(0, 1) + " "); //Gets first letter of each word
		}
		return sb.toString().toLowerCase();
	}
	public static void map(String file, String contents, List<MappedItem> mappedItems) {
		String[] words = contents.trim().split("\\s+");
		for(String word: words) {
			mappedItems.add(new MappedItem(word, file));
		}
	}

	public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for(String file: list) {
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
		for(String word: words) {
			results.add(new MappedItem(word, file));
		}
		callback.mapDone(file, results);
	}

	public static interface ReduceCallback<E, K, V> {

		public void reduceDone(E e, Map<K,V> results);
	}

	public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for(String file: list) {
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
