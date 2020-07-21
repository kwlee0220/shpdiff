package shpdiff;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeature;

import com.google.common.collect.Lists;

import marmot.Record;
import marmot.shp.ShapefileReader;
import marmot.shp.ShapefileWriter;
import utils.StopWatch;
import utils.func.FOption;
import utils.geo.Shapefile;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Test {
	private static final int COUNT = 10;
	private static final String SRID = "5186";
	private static final File OUTPUT = new File("/home/kwlee/tmp/output");
	private static final Charset EUC_KR = Charset.forName("euc-kr");
	
	public static final void main(String... args) throws Exception {
		long elapsed;
		
		Shapefile shp = Shapefile.of(Globals.MIDIUM);
		List<SimpleFeature> featureList = shp.streamFeatures().toList();
		SimpleFeatureCollection coll1 = new ListFeatureCollection(shp.getSimpleFeatureType(), featureList);
		
//		runATest(Globals.SMALL, 10);
//		System.out.println("SMALL: -----------------------------------------");
		
		runTestSuite();
		System.out.println("MEDIUM: -----------------------------------------");
//		
//		runATest(Globals.LARGE, 10);
//		System.out.println("LARGE: -----------------------------------------");
	}
	
	private static final void runTestSuite() throws Exception {
		runTestCase(false);
		runTestCase(true);
	}
	
	private static final long runTestCase(boolean record) throws Exception {
		long el = record ? runRecord() : runFeature();
		
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(COUNT);
		for ( int i =0; i < COUNT; ++i ) {
			long elapsed = record ? runRecord() : runFeature();
			elapseds.add(elapsed);
		}
		elapseds.sort((v1,v2) -> Long.compare(v1, v2));
		elapseds.remove(COUNT-1);
		elapseds.remove(0);
		long avg = Math.round(FStream.from(elapseds).mapToLong(v -> (Long)v).average().get());
		
		System.out.printf("avg: elapsed=%s%n", avg);
		
		return avg;
	}
	
	private static final long runFeature() throws Exception {
		StopWatch watch = StopWatch.start();
		
		Shapefile shp = Shapefile.of(Globals.MIDIUM);
		List<SimpleFeature> featureList = shp.streamFeatures().toList();
		SimpleFeatureCollection sfColl = new ListFeatureCollection(shp.getSimpleFeatureType(), featureList);
		
		shp.writeShapefile(OUTPUT, sfColl, Charset.defaultCharset(), FOption.empty(), FOption.empty());
		
		long elapsed = watch.stopInMillis();
		
		return elapsed;
	}
	
	private static final long runRecord() throws Exception {
		StopWatch watch = StopWatch.start();
		
		List<Record> recList = ShapefileReader.from(Globals.MIDIUM, EUC_KR).read().toList();
		ShapefileWriter.into(OUTPUT, "EPSG:4326", EUC_KR)
						.write(ShapefileReader.from(Globals.MIDIUM, EUC_KR).read());
		
		long elapsed = watch.stopInMillis();
		
		return elapsed;
	}
}
