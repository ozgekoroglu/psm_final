package org.processmining.scala.applications.mhs.bhs.t3.utils;

import java.io.*;

/**
 * @author nlvden
 */
final class BpiCsvSriptPostprocessor {

    private static final String LINE_START = "{{{";
    private static final String LINE_END = "}}}";
    private static final String SUFFIX = ".processed";

    public static void main(final String[] args) {
        try {
            final File dir = new File("C:\\T3\\BPI Logs\\ee");
            if (!dir.isDirectory()) {
                throw new IllegalArgumentException("A directory must be provided instead of " + dir.getAbsolutePath());
            }
            int n = 0;
            for (final File file : dir.listFiles()) {
                if (file.isFile() && file.getName().toLowerCase().endsWith(".csv")) {
                    processFile(file.getAbsolutePath());
                    n++;
                }
            }
            System.out.printf("%d files processed", n);

        } catch (Exception ex) {
            System.err.println(ex);
        }
    }

    static void processFile(final String filename) throws IOException {
        try {
            boolean started = false;
            StringBuilder sb = null;
            long lineNumber = 0;
            try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                try (PrintWriter writer = new PrintWriter( new BufferedWriter(new FileWriter(filename + SUFFIX)))) {
                    for (String _line; (_line = br.readLine()) != null; lineNumber++) {
                        final String line = _line.trim();
                        if (lineNumber != 0) {
                            if (!line.isEmpty()) {
                                if (!started) {
                                    if (line.startsWith(LINE_START)) {
                                        sb = new StringBuilder();
                                        if (!line.endsWith(LINE_END)) {
                                            sb.append(line.substring(LINE_START.length(), line.length()));
                                            started = true;
                                        } else { // {{{ }}}
                                            sb.append(line.substring(LINE_START.length(), line.length() - LINE_END.length()));
                                            writer.write(sb.toString() + "\r\n");
                                            sb = null; // to avoid hidden bugs
                                        }

                                    } else {
                                        throw new IllegalStateException(String.format("No line start in line %d", lineNumber));
                                    }
                                } else {
                                    if (line.startsWith(LINE_START)) {
                                        throw new IllegalStateException(String.format("Illegal line start in line %d", lineNumber));
                                    }
                                    if (line.endsWith(LINE_END)) {
                                        sb.append(line.substring(0, line.length() - LINE_END.length()));
                                        started = false;
                                        writer.write(sb.toString() + "\r\n");
                                        sb = null; // to avoid hidden bugs
                                    } else {
                                        sb.append(line);
                                    }
                                }
                            }
                        } else {
                            writer.write(line + "\r\n");
                        }
                    }
                }

            }
            System.out.printf("%d lines of '%s' are processed.\n", lineNumber, filename);
        } catch (Exception ex) {
            System.err.println("Cannot process file " + filename);
            throw ex;
        }
    }
}
