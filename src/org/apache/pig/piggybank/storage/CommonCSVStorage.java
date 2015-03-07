package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.PigException;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CommonCSVStorage extends PigStorage
        implements StoreFuncInterface {
    /* Common section */
    public CommonCSVStorage() {
        this(new String[0]);
    }

    private static CSVFormat processMasterParam(String arg) {
        CSVFormat result;
        if (arg.equals("DEFAULT"))
            result = CSVFormat.DEFAULT;
        else if (arg.equals("EXCEL"))
            result = CSVFormat.EXCEL;
        else if (arg.equals("MYSQL"))
            result = CSVFormat.MYSQL;
        else if (arg.equals("RFC4180"))
            result = CSVFormat.RFC4180;
        else if (arg.equals("TDF"))
            result = CSVFormat.TDF;
        else {
            if (arg.length() > 1)
                throw new IllegalArgumentException("First argument must be format name or a separator");
            result = CSVFormat.newFormat(arg.charAt(0));
        }
        return result;
    }

    private static CSVFormat processQuoteParam(CSVFormat result, String arg) {
        if (arg == null)
            result = result.withQuote(null);
        else {
            if (arg.length() > 1)
                throw new IllegalArgumentException("Quote argument must be NULL or a single character");
            result = result.withQuote(arg.charAt(0));
        }
        return result;
    }

    private static CSVFormat processQuoteModeParam(CSVFormat result, String arg) {
        QuoteMode quoteMode = QuoteMode.MINIMAL;
        if (arg == null || arg.equals("MINIMAL"))
            quoteMode = QuoteMode.MINIMAL;
        else if (arg.equals("ALL"))
            quoteMode = QuoteMode.ALL;
        else if (arg.equals("NON_NUMERIC"))
            quoteMode = QuoteMode.NON_NUMERIC;
        else if (arg.equals("NONE"))
            quoteMode = QuoteMode.NONE;
        else
            throw new IllegalArgumentException("Quote mode should be NULL or one of 'MINIMAL', 'ALL', 'NON_NUMERIC' or 'NONE'");
        result = result.withQuoteMode(quoteMode);
        return result;
    }

    private static CSVFormat processEscapeParam(CSVFormat result, String arg) {
        if (arg.equals("BACKSLASH"))
            arg = "\\";
        if (arg.length() > 1 || arg.length() == 0)
            throw new IllegalArgumentException("Only one escape character or 'BACKSLASH' may be given");
        result = result.withEscape(arg.charAt(0));
        return result;
    }

    private static CSVFormat processIgnoreSurroundingSpacesParam(CSVFormat result, String arg) {
        boolean ignoreSurroundingSpaces;
        if (arg == null || arg.equals("true"))
            ignoreSurroundingSpaces = true;
        else if (arg.equals("false"))
            ignoreSurroundingSpaces = false;
        else
            throw new IllegalArgumentException("'Ignore surrounding spaces' should be set to NULL=true or false");
        result = result.withIgnoreSurroundingSpaces(ignoreSurroundingSpaces);
        return result;
    }

    private static CSVFormat getCSVFormat(String... args) {
        CSVFormat result;

        if (args == null || args.length == 0)
            args = new String[]{"DEFAULT"};

        result = processMasterParam(args[0]);

        if (2 <= args.length)
            result = processQuoteParam(result, args[1]);

        if (3 <= args.length)
            result = processQuoteModeParam(result, args[2]);

        if (4 <= args.length)
            result = processEscapeParam(result, args[3]);

        if (5 <= args.length)
            result = result.withNullString(args[4]);

        if (6 <= args.length)
            result = processIgnoreSurroundingSpacesParam(result, args[5]);

        if (7 <= args.length)
            throw new IllegalArgumentException("Too many parameters");

        result = result.withRecordSeparator("");

        return result;
    }

    public CommonCSVStorage(String... args) throws IllegalArgumentException {
        csvFormat = getCSVFormat(args);
    }

    /* Load section */
    protected RecordReader<Object, Object> reader = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private String loadLocation;

    private CSVFormat csvFormat;

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() {
        if (loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new PigTextInputFormat();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) {
        reader = (RecordReader<Object, Object>) recordReader;
    }

    /* Reads a single row */
    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }
            Text value = (Text) reader.getCurrentValue();

            CSVParser parser = CSVParser.parse(value.toString(), this.csvFormat);

            Tuple result = null;
            /* This FOR is supposed to run only once. Effectively, this means
             * that only the last record is addressed */
            for (CSVRecord csvRecord : parser.getRecords()) {
                if (result == null)
                    result = tupleFactory.newTuple(csvRecord.size());
                else {
                    throw new ExecException("A record can only have one CSV row", 6018,
                            PigException.REMOTE_ENVIRONMENT);
                }
                int numColumns = csvRecord.size();
                for (int i = 0; i < numColumns; i++) {
                    result.set(i, csvRecord.get(i));
                }
            }

            return result;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    /* Write section */
    protected RecordWriter<Object, Object> writer = null;

    @SuppressWarnings("rawtypes")
    @Override
    public OutputFormat getOutputFormat() {
        return new TextOutputFormat<LongWritable, Text>();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void prepareToWrite(RecordWriter recordWriter) {
        writer = (RecordWriter<Object, Object>) recordWriter;
    }

    /* Checks if a all tuple fields are empty */
    private boolean isTupleEmpty(Tuple tuple) {
        for (Object col : tuple) {
            if (col != null && !col.toString().isEmpty()) {
                return false;
            }
        }

        return true;
    }

    /* Writes a single row */
    @Override
    public void putNext(Tuple tuple) throws IOException {

        if (this.isTupleEmpty(tuple))
            return;

        StringWriter strWriter = new StringWriter();

        CSVPrinter csvPrinter = new CSVPrinter(strWriter, this.csvFormat);

        csvPrinter.printRecord(tuple);

        try {
            writer.write(null, strWriter.toString());

            strWriter.close();
            csvPrinter.close();

        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while storing output";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }

}

