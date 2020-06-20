package shpdiff;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import utils.StopWatch;
import utils.jdbc.JdbcProcessor;
import utils.record.Record;
import utils.record.RecordSchema;
import utils.record.RecordSet;
import utils.record.geotools.ShapefileRecordSetReader;
import utils.record.jdbc.GeometryFormat;
import utils.record.jdbc.JdbcRecordAdaptor;
import utils.record.jdbc.JdbcRecordSetWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfWriteToDB {
	public static final void main(String... args) throws Exception {
		long elapsed;
		
		elapsed = runATest(Globals.SMALL, 10);
		System.out.printf("SMALL: elapsed=%dms%n", elapsed);
		
		elapsed = runATest(Globals.MIDIUM, 10);
		System.out.printf("MEDIUM: elapsed=%dms%n", elapsed);
		
		elapsed = runATest(Globals.LARGE, 10);
		System.out.printf("LARGE: elapsed=%dms%n", elapsed);
	}
	
	private static final long runATest(File shpFile, int count) throws Exception {
		ShapefileRecordSetReader reader = ShapefileRecordSetReader.from(shpFile, Charset.forName("euc-kr"));
		RecordSchema schema = reader.getRecordSchema();
		
		List<Record> recordList = reader.read().toList();
		JdbcProcessor jdbc = JdbcProcessor.create("postgresql", "129.254.82.42", 5432, "kwlee", "urc2004", "kwlee");
		JdbcRecordAdaptor adaptor = JdbcRecordAdaptor.create(jdbc, schema, GeometryFormat.NATIVE);

		store(shpFile, jdbc, adaptor, recordList);
		StopWatch watch = StopWatch.start();
		store(shpFile, jdbc, adaptor, recordList);
		watch.stop();
		
		adaptor.deleteTable(jdbc, "test_perf");
		
		return watch.getElapsedInMillis();
	}
	
	private static final void store(File shpFile, JdbcProcessor jdbc, JdbcRecordAdaptor adaptor,
									List<Record> recList) throws Exception {
		try ( JdbcRecordSetWriter writer = new JdbcRecordSetWriter(jdbc, "test_perf", adaptor); ) {
			writer.setForce(true);
			long cnt = writer.write(RecordSet.from(recList));
			System.out.println("count=" + cnt);
		}
	}
}
