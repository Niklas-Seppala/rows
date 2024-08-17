package org.ns;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TemperatureProcessor {

    public static void main(String[] args) throws Exception {
        processFile("rows-tiny.data");
    }

    public static void processFile(@NotNull String file) throws Exception {
        final int threads = Runtime.getRuntime().availableProcessors();
        BlockingQueue<Map<String, Temperature>> resQueue = new ArrayBlockingQueue<>(threads);

        try (FileChannel channel = FileChannel.open(Path.of(file),
                StandardOpenOption.READ)) {
            Thread[] runners = new Thread[threads];

            final MemorySegment mmap = channel.map(FileChannel.MapMode.READ_ONLY,
                    0,
                    channel.size(),
                    Arena.global());
            long segment = 0;
            long segmentSize = channel.size() / threads;
            Runner prevRunner = null;
            for (int i = 0; i < threads; i++) {
                Runner runner = new Runner(segment, segment + segmentSize, resQueue, mmap, prevRunner);
                runners[i] = new Thread(runner);
                runners[i].start();
                segment += segmentSize;
                prevRunner = runner;
            }

            Map<String, Temperature> finalResult = new ConcurrentHashMap<>(Runner.MAP_SIZE);
            for (int i = 0; i < threads; i++) {
                Map<String, Temperature> result =  resQueue.poll(120, TimeUnit.SECONDS);
                assert result != null;
                result.entrySet().stream().parallel()
                        .forEach((entry) -> finalResult.merge(entry.getKey(), entry.getValue(), Temperature::merge));
            }
            writeResultsTo(System.out, finalResult);
        }
    }

    private static void writeResultsTo(final PrintStream out,
                                       final Map<String, Temperature> results) {
        out.print('{');
        results.forEach((city, data) -> {
            data.printToOut(out, city);
            out.print(',');
            out.print(' ');
        });
        out.print('\b');
        out.print('\b');
        out.print('}');
        out.print('\n');
    }

    private static class Temperature {
        float high; double mean; float low; double sum; int count;

        public Temperature(float initial) {
            this.sum = this.mean = this.low = this.high = initial;
            this.count = 1;
        }

        public Temperature merge(Temperature other) {
            if (other.high > this.high) this.high = other.high;
            if (other.low < this.low) this.low = other.low;
            this.sum += other.sum;
            this.count += other.count;
            this.mean = sum / count;
            return this;
        }

        public Temperature merge(float temperature) {
            if (temperature > this.high) this.high = temperature;
            if (temperature < this.low) this.low = temperature;
            this.sum += temperature;
            this.count++;
            return this;
        }

        private void printToOut(final PrintStream out, String key) {
            out.printf("%s=%.1f/%.1f/%.1f", key, high, mean, low);
        }
    }

    private static class Runner implements Runnable {
        static final int BUFFER_SIZE = 256;
        static final byte NEWLINE = 10;
        static final byte CITY_DELIM = 59;
        static final int MAP_SIZE = 1 << 14;
        private final long start;
        private long end;
        @NotNull
        private final BlockingQueue<Map<String, Temperature>> resultQueue;
        @NotNull
        private final MemorySegment mmap;
        private final HashMap<String, Temperature> map = new HashMap<>(MAP_SIZE);
        @Nullable
        private final Runner previousRunner;
        private final byte[] buffer = new byte[BUFFER_SIZE];

        Runner(long start, long end,
               @NotNull BlockingQueue<Map<String, Temperature>> resultQueue,
               @NotNull MemorySegment mmap,
               @Nullable Runner previousRunner) {
            this.start = start;
            this.end = end;
            this.resultQueue = resultQueue;
            this.mmap = mmap;
            this.previousRunner = previousRunner;
        }


        /**
         * @param length
         */
        private void parse(int length) {
            String city = null;
            float temperature = Integer.MIN_VALUE;
            for (int cursor = 0; cursor < length; cursor++) {
                byte c = buffer[cursor];
                if (c == CITY_DELIM) {
                    city = new String(buffer, 0, cursor);

                    temperature = Float.parseFloat(new String(buffer, cursor+1,
                            length - cursor-1));
                    break;
                }
            }

            assert city != null;
            assert temperature != Integer.MIN_VALUE;
            final float temp = temperature;
            map.compute(city, (k, v) -> {
                if (v == null) return new Temperature(temp);
                return v.merge(temp);
            });
        }

        public synchronized void setRewind(int offset) {
            this.end -= offset;
        }

        @Override
        public void run() {
            long cursor = start;
            if (this.previousRunner != null) {
                // Seek the real start position. File mapping is partitioned by bytes,
                // not by lines:
                //      1. Rewind until newline is found
                //      2. Communicate the change in limits for thread
                //         handling previous segment
                short rewindOffset = -1;
                while ((mmap.get(ValueLayout.JAVA_BYTE, cursor - rewindOffset)) != NEWLINE) {
                    rewindOffset++;
                }
                rewindOffset -= 1;
                cursor -= rewindOffset;
                previousRunner.setRewind(rewindOffset);
            }
            int bufferCursor = 0;
            while (cursor < end) {
                byte c = mmap.get(ValueLayout.JAVA_BYTE, cursor++);
                if (c != NEWLINE) {
                    assert bufferCursor < BUFFER_SIZE : "read-buffer overflow";
                    buffer[bufferCursor++] = c;
                } else {
                    parse(bufferCursor);
                    bufferCursor = 0;
                }
            }
            try {
                resultQueue.put(map);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

