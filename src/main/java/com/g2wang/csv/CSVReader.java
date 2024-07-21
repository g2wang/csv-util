package com.g2wang.csv;

import java.util.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class CSVReader implements AutoCloseable {
    private enum State {NOT_ESCAPED, END_OF_ROW, END_OF_FIELD, ILLEGAL,
        ONE_QUOTE, DOUBLE_QUOTES, QUOTE_NOT_QUOTES, QUOTE_NOT_QUOTES_QUOTE}
    private enum Char { QUOTE, COMMA, CR, NL, OTHER}
    private enum Operation {APPEND, NONE, WRITE_FIELD, WRITE_ROW, THROW_EXCEPTION}
    private static char cma = ',';

    private static Map<Integer, Char> chars = new HashMap<>();
    static {
        chars.put((int)'"', Char.QUOTE);
        chars.put((int) cma, Char.COMMA);
        chars.put((int)'\r', Char.CR);
        chars.put((int)'\n', Char.NL);
    }

    private static EnumMap<Char, EnumMap<State, StateAndOperation>> table
        = new EnumMap<>(Char.class);

    static {
        Char inputChar = Char.QUOTE;
        EnumMap<State, StateAndOperation> transitionMap
            = new EnumMap<>(State.class);

        State currentState = State.NOT_ESCAPED;
        StateAndOperation gotoStateWithOp = new StateAndOperation(State.ONE_QUOTE, Operation.NONE);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.ONE_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.DOUBLE_QUOTES, Operation.NONE);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES_QUOTE, Operation.NONE);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.DOUBLE_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.ONE_QUOTE, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.ONE_QUOTE, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        table.put(inputChar, transitionMap);

        //////
        inputChar = Char.COMMA;
        transitionMap = new EnumMap<>(State.class);

        currentState = State.NOT_ESCAPED;
        gotoStateWithOp = new StateAndOperation(State.END_OF_FIELD, Operation.WRITE_FIELD);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.ONE_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.DOUBLE_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.END_OF_FIELD, Operation.WRITE_FIELD);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.END_OF_FIELD, Operation.WRITE_FIELD);
        transitionMap.put(currentState, gotoStateWithOp);

        table.put(inputChar, transitionMap);

        ///////
        inputChar = Char.CR;
        transitionMap = new EnumMap<>(State.class);

        currentState = State.NOT_ESCAPED;
        gotoStateWithOp = new StateAndOperation(State.NOT_ESCAPED, Operation.NONE);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.ONE_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.DOUBLE_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.DOUBLE_QUOTES, Operation.NONE);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES_QUOTE, Operation.NONE);
        transitionMap.put(currentState, gotoStateWithOp);

        table.put(inputChar, transitionMap);

        /////////
        inputChar = Char.NL;
        transitionMap = new EnumMap<>(State.class);

        currentState = State.NOT_ESCAPED;
        gotoStateWithOp = new StateAndOperation(State.END_OF_ROW, Operation.WRITE_ROW);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.ONE_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.DOUBLE_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.END_OF_ROW, Operation.WRITE_ROW);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.END_OF_ROW, Operation.WRITE_ROW);
        transitionMap.put(currentState, gotoStateWithOp);

        table.put(inputChar, transitionMap);

        /////////
        inputChar = Char.OTHER;
        transitionMap = new EnumMap<>(State.class);

        currentState = State.NOT_ESCAPED;
        gotoStateWithOp = new StateAndOperation(State.NOT_ESCAPED, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.ONE_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.QUOTE_NOT_QUOTES, Operation.APPEND);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.DOUBLE_QUOTES;
        gotoStateWithOp = new StateAndOperation(State.ILLEGAL, Operation.THROW_EXCEPTION);
        transitionMap.put(currentState, gotoStateWithOp);

        currentState = State.QUOTE_NOT_QUOTES_QUOTE;
        gotoStateWithOp = new StateAndOperation(State.ILLEGAL, Operation.THROW_EXCEPTION);
        transitionMap.put(currentState, gotoStateWithOp);

        table.put(inputChar, transitionMap);

    }

    private static class StateAndOperation {
        private State state;
        private Operation operation;

        public StateAndOperation(State state, Operation operation) {
            this.state = state;
            this.operation = operation;
        }

        public String toString() {
            return "state=" + state + "\n"
                + "operation=" + operation + "\n";
        }
    }

    private Reader reader = null;

    /**
     * constructor
     */
    protected CSVReader() {
    }

    /**
     * static method to get an instance to read a CSV InputStream
     */
    public static CSVReader fromInputStream(final InputStream inputStream) {
        CSVReader csvReader = new CSVReader();
        csvReader.reader = new InputStreamReader(inputStream);
        return csvReader;
    }

    /**
     * static method to get an instance to read a CSV InputStream with specified Charset
     */
    public static CSVReader fromInputStream(final InputStream inputStream, Charset cs) {
        CSVReader csvReader = new CSVReader();
        csvReader.reader = new InputStreamReader(inputStream, cs);
        return csvReader;
    }

    /**
     * static method to get an instance to read a CSV InputStream with specified CharsetSecorder
     */
    public static CSVReader fromInputStream(final InputStream inputStream, CharsetDecoder dec) {
        CSVReader csvReader = new CSVReader();
        csvReader.reader = new InputStreamReader(inputStream, dec);
        return csvReader;
    }

    /**
     * static method to get an instance to read a CSV InputStream with specified charsetName
     */
    public static CSVReader fromInputStream(final InputStream inputStream, String charsetName)
            throws UnsupportedEncodingException {
        CSVReader csvReader = new CSVReader();
        csvReader.reader = new InputStreamReader(inputStream, charsetName);
        return csvReader;
    }

    /**
     * static method to get an instance to read a CSV file
     * @param csvFile - csv file path
     */
    public static CSVReader fromFile(final String csvFile)
        throws FileNotFoundException {
        return fromInputStream(new FileInputStream(csvFile));
    }

    /**
     * static method to get an instance to read a CSV file
     * @param csvFile - csv file
     */
    public static CSVReader fromFile(final File csvFile)
        throws FileNotFoundException {
        return fromInputStream(new FileInputStream(csvFile));
    }

    /**
     * static method to get an instance to read a CSV file with specified Charset
     * @param csvFile - csv file path
     * @param charset - charset
     */
    public static CSVReader fromFile(final String csvFile, final Charset charset)
            throws FileNotFoundException {
        return fromInputStream(new FileInputStream(csvFile), charset);
    }

    /**
     * static method to get an instance to read a CSV file with specified Charset
     */
    public static CSVReader fromFile(final File csvFile, final Charset charset)
            throws FileNotFoundException {
        return fromInputStream(new FileInputStream(csvFile), charset);
    }

    /**
     * static method to get an instance to read a CSV file with specified CharsetDecoder
     */
    public static CSVReader fromFile(final String csvFile, final CharsetDecoder dec)
            throws FileNotFoundException {
        return fromInputStream(new FileInputStream(csvFile), dec);
    }

    /**
     * static method to get an instance to read a CSV file with specified CharsetDecoder
     */
    public static CSVReader fromFile(final File csvFile, final CharsetDecoder dec)
            throws FileNotFoundException {
        return fromInputStream(new FileInputStream(csvFile), dec);
    }

    /**
     * static method to get an instance to read a CSV file with specified charsetName, e.g. utf-8
     */
    public static CSVReader fromFile(final String csvFile, final String charsetName)
            throws FileNotFoundException, UnsupportedEncodingException {
        return fromInputStream(new FileInputStream(csvFile), charsetName);
    }

    /**
     * static method to get an instance to read a CSV file with specified charsetName, e.g. utf-8
     */
    public static CSVReader fromFile(final File csvFile, final String charsetName)
        throws FileNotFoundException, UnsupportedEncodingException {
        return fromInputStream(new FileInputStream(csvFile), charsetName);
    }

    /**
     * static method to get an instance to read a CSV String
     * @param csvString - csv string
     */
    public static CSVReader fromString(final String csvString) {
        CSVReader csvReader = new CSVReader();
        csvReader.reader = new StringReader(csvString);
        return csvReader;
    }

    /**
     * instance method to get the next row from a CVS input source 
     * @return a row as an List of String
     *          or null if no more row is found
     * @throws IllegalCSVFormatException, IOException
     */
    public List<String> nextRow()
        throws IllegalCSVFormatException, IOException {

        int readInt = -1;
        readInt = reader.read();
        if (readInt == -1) {
            return null;
        }
        List<String> row = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        State aState = State.NOT_ESCAPED;
        long count = 0;
        while (readInt >= 0) {
            Char aChar = chars.get(readInt);
            if (aChar == null) {
                aChar = Char.OTHER;
            }

            StateAndOperation stOp = table.get(aChar).get(aState);

            aState = stOp.state;
            Operation op = stOp.operation;

            switch (op) {
                case APPEND:
                    sb.append((char)readInt);
                    break;
                case WRITE_FIELD:
                    sb.append("");
                    row.add(sb.toString());
                    sb.delete(0, sb.length());
                    aState = State.NOT_ESCAPED;
                   break;
                case WRITE_ROW:
                    sb.append("");
                    row.add(sb.toString());
                    return row;
                case THROW_EXCEPTION:
                    throw new IllegalCSVFormatException("Illegal CSV Format at char " + (count+1) + ".");
                default:
                    //do nothing
                    break;
            }

            readInt = reader.read();
            count++;
        }

        sb.append("");
        row.add(sb.toString());
        return row;
    }

    /**
     * release resources
     */
    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                //do nothing
            } finally {
                reader = null;
            }
        }
    }

    /**
     * set the delimiter if it is not comma
     * @param delimiter - the delimiter, default comma (,)
     */
    public static void setDelimiter(char delimiter) {
        chars.remove((int) cma);
        cma = delimiter;
        chars.put((int) cma, Char.COMMA);
    }
}