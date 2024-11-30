package pwio;

import com.opencsv.CSVReader;


import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Main {
    public static void main(String[] args) {
        String filePath = "D:\\Studia\\sem9\\pwio\\proj\\Airbnb_Open_Data.csv";
        long start = System.nanoTime();
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            Stream<String[]> rowsStream = StreamSupport.stream(csvReader.spliterator(), true);

            Map<String, Map<String, Long>> listingsCount = rowsStream.skip(1)
                    .filter(row -> {
                        try {
                            counter.incrementAndGet();
                            return Integer.parseInt(row[CsvConst.PRICE].replace("$", "").trim().replace(",", "").trim()) < 1000;
                        } catch (NumberFormatException e) {
                            failed.incrementAndGet();
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

            listingsCount.entrySet().stream().forEach(entry -> {
                String group = entry.getKey();
                Map<String, Long> hoods = entry.getValue();
                hoods.entrySet().stream().forEach(entry2 -> {
                            String hoodName = entry2.getKey();
                            Long count = entry2.getValue();
                            System.out.println("Group: " + group + ", hood: " + hoodName + ", count: " + count);
                        }
                );
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println((System.nanoTime() - start)/1000000);
        System.out.println(counter.toString());
        System.out.println(failed.toString());
    }
}