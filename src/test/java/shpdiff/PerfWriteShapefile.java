package shpdiff;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeature;

import com.google.common.collect.Lists;

import utils.StopWatch;
import utils.func.FOption;
import utils.geo.Shapefile;
import utils.geo.SimpleFeatures;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfWriteShapefile {
	private static final int COUNT = 10;
	private static final File OUTPUT = new File("C:/Temp/output");
	
	public static final void main(String... args) throws Exception {
		long elapsed;
		
		runATest(Globals.SMALL, 10);
		System.out.println("SMALL: -----------------------------------------");
		
		runATest(Globals.MIDIUM, 10);
		System.out.println("MEDIUM: -----------------------------------------");
		
		runATest(Globals.LARGE, 10);
		System.out.println("LARGE: -----------------------------------------");
	}
	
	private static final void runATest(File shpFile, int count) throws Exception {
		Shapefile shp = Shapefile.of(shpFile, Charset.forName("euc-kr"));
		
		List<SimpleFeature> featureList;
		for ( float ratio: new float[] {0.01f, 0.1f, 0.3f, 0.7f, 1f} ) {
			featureList = shp.streamFeatures().sample(ratio).toList();
			runWithRatio(featureList, ratio);
		}
	}
	
	private static final long runWithRatio(List<SimpleFeature> recordList, float ratio) throws Exception {
		write(recordList, OUTPUT);
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(COUNT);
		for ( int i =0; i < COUNT; ++i ) {
			elapseds.add(write(recordList, OUTPUT));
		}
		elapseds.sort((v1,v2) -> Long.compare(v1, v2));
		elapseds.remove(COUNT-1);
		elapseds.remove(0);
		long avg = Math.round(FStream.from(elapseds).mapToLong(v -> (Long)v).average().get());
		
		System.out.printf("avg: ratio=%.0f, count=%d, elapsed=%s%n",
							ratio*100, recordList.size(), avg);
		
		return avg;
	}
	
	private static final long write(List<SimpleFeature> featureList, File outDir) throws Exception {
		StopWatch watch = StopWatch.start();
		SimpleFeatureCollection sfColl = SimpleFeatures.toFeatureCollection(featureList);
		Shapefile.writeShapefile(outDir, sfColl, Charset.forName("euc-kr"),
								FOption.empty(), FOption.empty());
		long elapsed = watch.stopInMillis();
		System.out.printf("\telapsed=%s%n", elapsed);
		
		return elapsed;
	}
}
