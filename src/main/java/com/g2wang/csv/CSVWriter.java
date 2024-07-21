package com.g2wang.csv;

import java.io.*;
import java.util.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

public class CSVWriter implements AutoCloseable {
    private BufferedWriter writer = null;
    /**
     * constructor
     */
    protected CSVWriter() {
    }

    /**
     * static method to create an instance of CSVWriter to write to an OutputStream
     */
    public static CSVWriter toOutputStream(OutputStream outputStream) {
        CSVWriter csvWriter = new CSVWriter();
        csvWriter.writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        return csvWriter;
    }

    /**
     * static method to create an instance of CSVWriter to writer to an OutputStream with specified Charset
     */
    public static CSVWriter toOutputStream(OutputStream outputStream, Charset cs) {
        CSVWriter csvWriter = new CSVWriter();
        csvWriter.writer = new BufferedWriter(new OutputStreamWriter(outputStream, cs));
        return csvWriter;
    }

    /**
     * static method to create an instance of CSVWriter to write to an OutputStream with specified CharsetEncoder
     */
    public static CSVWriter toOutputStream(OutputStream outputStream, CharsetEncoder enc) {
        CSVWriter csvWriter = new CSVWriter();
        csvWriter.writer = new BufferedWriter(new OutputStreamWriter(outputStream, enc));
        return csvWriter;
    }

    /**
     * static method to create an instance of CSVWriter to write to an OutputStream with specified charsetName
     */
    public static CSVWriter toOutputStream(OutputStream outputStream, String charsetName) throws UnsupportedEncodingException {
        CSVWriter csvWriter = new CSVWriter();
        csvWriter.writer = new BufferedWriter(new OutputStreamWriter(outputStream, charsetName));
        return csvWriter;
    }

    /**
     * static method to create an instance to write to a csv file
     */
    public static CSVWriter toFile(final String csvFile) throws IOException {
        createFileIfNotExists(csvFile);
        return toOutputStream(new FileOutputStream(csvFile));
    }

    /**
     * static method to create an instance to write to a csv file
     */
    public static CSVWriter toFile(final File csvFile) throws IOException {
        createFileIfNotExists(csvFile);
        return toOutputStream(new FileOutputStream(csvFile));
    }

    /**
     * static method to create an instance to write to a csv file with specified charsetName
     */
    public static CSVWriter toFile(final String csvFile, final String charsetName) throws IOException {
        createFileIfNotExists(csvFile);
        return toOutputStream(new FileOutputStream(csvFile), charsetName);
    }

    /**
     * static method to create an instance to write to a csv file with specified charsetName
     */
    public static CSVWriter toFile(final File csvFile, final String charsetName) throws IOException {
        createFileIfNotExists(csvFile);
        return toOutputStream(new FileOutputStream(csvFile), charsetName);
    }

    /**
     * static method to create an instance to write to a csv file with specified Charset
     */
    public static CSVWriter toFile(final String csvFile, final Charset charset) throws IOException {
        createFileIfNotExists(csvFile);
        return toOutputStream(new FileOutputStream(csvFile), charset);
    }

    /**
     * static method to create an instance to write to a csv file with specified Charset
     */
    public static CSVWriter toFile(final File csvFile, final Charset charset) throws IOException {
        createFileIfNotExists(csvFile);
        return toOutputStream(new FileOutputStream(csvFile), charset);
    }

    /**
     * static method to create an instance to write to a csv file with specified CharsetEncoder
     */
    public static CSVWriter toFile(final String csvFile, final CharsetEncoder enc) throws IOException {
        createFileIfNotExists(csvFile);
        return toOutputStream(new FileOutputStream(csvFile), enc);
    }

    /**
     * static method to create an instance to write to a csv file with specified CharsetEncoder
     */
    public static CSVWriter toFile(final File csvFile, final CharsetEncoder enc) throws IOException {
        createFileIfNotExists(csvFile);
        return toOutputStream(new FileOutputStream(csvFile), enc);
    }

    /**
     * static method to create an instance of CSVWriter to write to a StringWriter
     */
    public static CSVWriter toStringWriter(StringWriter stringWriter) {
        CSVWriter csvWriter = new CSVWriter();
        csvWriter.writer = new BufferedWriter(stringWriter);
        return csvWriter;
    }

    private static void createFileIfNotExists(String fileName) throws IOException {
        File file = new File(fileName);
        createFileIfNotExists(file);
    }

    private static void createFileIfNotExists(File file) throws IOException {
        if (!file.exists() && !file.createNewFile()) {
            throw new IllegalStateException("file cannot be created");
        }
    }

    public void write(String[] fields) throws IOException {
        List<String> list = Arrays.asList(fields);
        write(list);
    }

    public void write(Iterable<String> fields) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (String f : fields) {
            sb.append(quote(f)).append(',');
        }
        String s = sb.substring(0, sb.length() - 1);
        writer.write(s);
        writer.newLine();
    }

    /**
     *
     */
    @Override
    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                // do nothing
            } finally {
                writer = null;
            }
        }
    }

    private static String quote(String f) {
        if (f == null || f.isEmpty()) {
            return f;
        }
        for (char c : f.toCharArray()) {
            if (c == ',' || c == '"' || c == '\'' || c == '\n' || c == '\r') {
                return "\"" + f.replace("\"", "\"\"") + "\"";
            }
        }
        return f;
    }
}
