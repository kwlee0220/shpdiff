package shpdiff;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import utils.StopWatch;
import utils.func.Try;
import utils.jdbc.JdbcProcessor;
import utils.record.Record;
import utils.record.RecordSchema;
import utils.record.RecordSet;
import utils.record.geotools.ShapefileRecordSetReader;
import utils.record.geotools.SimpleFeatures;
import utils.record.jdbc.GeometryFormat;
import utils.record.jdbc.JdbcRecordAdaptor;
import utils.record.jdbc.JdbcRecordSetWriter;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfWriteToDB {
	public static final void main(String... args) throws Exception {
		long elapsed;
		
		elapsed = runATest(Globals.SMALL, 3);
		System.out.printf("SMALL: elapsed=%dms%n", elapsed);
		
		elapsed = runATest(Globals.MIDIUM, 3);
		System.out.printf("MEDIUM: elapsed=%dms%n", elapsed);
		
		elapsed = runATest(Globals.LARGE, 3);
		System.out.printf("LARGE: elapsed=%dms%n", elapsed);
	}
	
	private static final long runATest(File shpFile, int count) throws Exception {
		ShapefileRecordSetReader reader = SimpleFeatures.readShapefile(shpFile, Charset.forName("euc-kr"));
		RecordSchema schema = reader.getRecordSchema();
		
		List<Record> recordList = reader.read().toList();
		JdbcProcessor jdbc = JdbcProcessor.create("postgresql", "129.254.82.42", 5432, "kwlee", "urc2004", "kwlee");
		JdbcRecordAdaptor adaptor = JdbcRecordAdaptor.create(jdbc, schema, GeometryFormat.NATIVE);
		store(shpFile, jdbc, adaptor, recordList);
		
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(count);
		for ( int i =0; i < count; ++i ) {
			elapseds.add(store(shpFile, jdbc, adaptor, recordList));
		}
		adaptor.deleteTable("test_perf");
		elapseds.sort((v1,v2) -> Long.compare(v1, v2));
		elapseds.remove(count-1);
		elapseds.remove(0);
		
		return Math.round(FStream.from(elapseds).mapToLong(v -> (Long)v).average().get());
	}
	
	private static final long store(File shpFile, JdbcProcessor jdbc, JdbcRecordAdaptor adaptor,
									List<Record> recList) throws Exception {
		Try.run(() -> adaptor.deleteTable("test_perf"));
		adaptor.createTable("test_perf", Collections.emptyList(), Collections.emptyList());
		
		StopWatch watch = StopWatch.start();
		try ( JdbcRecordSetWriter writer = new JdbcRecordSetWriter(jdbc, "test_perf", adaptor); ) {
			writer.write(RecordSet.from(recList));
		}
		watch.stop();
		System.out.println("elapsed=" + watch.getElapsedInMillis());
		
		return watch.getElapsedInMillis();
	}
}
