package shpdiff;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import org.opengis.feature.simple.SimpleFeature;

import com.google.common.collect.Lists;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.jdbc.GeometryFormat;
import marmot.jdbc.JdbcDataSetWriter;
import marmot.jdbc.JdbcRecordAdaptor;
import record.shp.ShapefileReader;
import utils.StopWatch;
import utils.func.Try;
import utils.geo.Shapefile;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfWriteToDB {
	private static final int TEST_COUNT = 10;
	private static final String SRID = "5186";
	private static final Charset EUC_KR = Charset.forName("euc-kr");
	
	public static final void main(String... args) throws Exception {
		long elapsed;
		
		runATest(Globals.SMALL, "SMALL");
		runATest(Globals.MIDIUM, "MIDIUM");
		runATest(Globals.LARGE, "LARGE");
	}
	
	private static final void runATest(File shpFile, String symbol) throws Exception {
		Shapefile shp = Shapefile.of(shpFile, Charset.forName("euc-kr"));
		
		List<SimpleFeature> featureList;
		for ( float ratio: new float[] {0.01f, 0.1f, 0.3f, 0.7f, 1f} ) {
			long elapsed = runATestWithRatio(shpFile, ratio, TEST_COUNT);
			System.out.printf("%s: ratio=%.0f%%, elapsed=%dms%n", symbol, ratio*100, elapsed);
		}
	}
	
	private static final long runATestWithRatio(File shpFile, float ratio, int count) throws Exception {
		ShapefileReader ds = ShapefileReader.from(shpFile, EUC_KR);
		RecordSchema schema = ds.getRecordSchema();
		
		List<Record> recordList = ds.read().fstream().sample(ratio).toList();
		
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
		JdbcDataSetWriter writer = new JdbcDataSetWriter(adaptor, "test_perf");
		writer.write(RecordStream.from(recList));
		watch.stop();
//		System.out.println("elapsed=" + watch.getElapsedInMillis());
		
		return watch.getElapsedInMillis();
	}
}
