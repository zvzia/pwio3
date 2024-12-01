package pwio;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PerformanceStats {
    public static int DATA_MULTIPLY_FACTOR = 10;
    public static int CALL_REPETITION = 10;


    public static void main(String[] args) {
        String filePath = "D:\\Studia\\sem9\\pwio\\proj\\Airbnb_Open_Data.csv";

        List<String[]> rows = new ArrayList<>();
        try {
            CSVReader reader = new CSVReader(new FileReader(filePath));
            reader.spliterator().forEachRemaining(rows::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        rows.remove(0);

        List<String[]> multipliedRows = new ArrayList<>();
        for (int i = 0; i < DATA_MULTIPLY_FACTOR; i++) {
            multipliedRows.addAll(rows);
        }

        processData(rows, false); //ignore first call

        long sequentialTime = 0;
        long parallelTime = 0;
        for (int i = 0; i < CALL_REPETITION; i++) {
            sequentialTime += processData(multipliedRows, false);
        }
        for (int i = 0; i < CALL_REPETITION; i++) {
            parallelTime += processData(multipliedRows, true);
        }

        System.out.println("Average sequential time: " + sequentialTime / CALL_REPETITION / 1000000 + " milliseconds");
        System.out.println("Average parallel time: " + parallelTime / CALL_REPETITION / 1000000 + " milliseconds");

    }

    public static long processData(List<String[]> rows, boolean isParallel) {

        Stream<String[]> rowsStream = null;
        if (isParallel) rowsStream = rows.parallelStream();
        else rowsStream = rows.stream();

        long start = System.nanoTime();
        Map<String, Map<String, Long>> listingsCount = rowsStream
                .filter(row -> {
                    try {
                        return Integer.parseInt(row[CsvConst.PRICE].replace("$", "").trim().replace(",", "").trim()) < 1000;
                    } catch (NumberFormatException e) {
                        return false;
                    }
                })
                .filter(row -> !Objects.equals(row[CsvConst.NEIGHBOURHOOD_GROUP], "") && !Objects.equals(row[CsvConst.NEIGHBOURHOOD], ""))
                .map(row -> new AbstractMap.SimpleEntry<>(row[CsvConst.NEIGHBOURHOOD_GROUP], row[CsvConst.NEIGHBOURHOOD]))
                .sorted((o1, o2) -> {
                    int keyCompare = o2.getKey().compareTo(o1.getKey());
                    return keyCompare == 0 ? o1.getValue().compareTo(o2.getValue()) : keyCompare;
                })
                .collect(Collectors.groupingBy(AbstractMap.SimpleEntry::getKey, Collectors.groupingBy(AbstractMap.SimpleEntry::getValue, LinkedHashMap::new, Collectors.counting())));
        long end = System.nanoTime();

        /*listingsCount.entrySet().stream().forEach(entry -> {
            String group = entry.getKey();
            Map<String, Long> hoods = entry.getValue();
            hoods.entrySet().stream().forEach(entry2 -> {
                        String hoodName = entry2.getKey();
                        Long count = entry2.getValue();
                        System.out.println("Group: " + group + ", hood: " + hoodName + ", count: " + count);
                    }
            );
        });*/

        System.out.println((end - start) / 1000000 + " milliseconds");
        return end - start;
    }
}
