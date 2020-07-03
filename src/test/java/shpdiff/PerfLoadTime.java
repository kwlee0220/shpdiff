package shpdiff;

import java.io.File;
import java.util.List;

import org.opengis.feature.simple.SimpleFeature;

import com.google.common.collect.Lists;

import utils.StopWatch;
import utils.func.Tuple;
import utils.func.Tuple3;
import utils.geo.Shapefile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfLoadTime {
	public static final void main(String... args) throws Exception {
		Tuple<Long,Long> result;
		
		result = runATest(Globals.SMALL, 10);
		System.out.printf("SMALL: elapsed=%dms, memory=%d%n", result._1, result._2);
		
		result = runATest(Globals.MIDIUM, 10);
		System.out.printf("MEDIUM: elapsed=%dms, memory=%d%n", result._1, result._2);
		
		result = runATest(Globals.LARGE, 10);
		System.out.printf("LARGE: elapsed=%dms, memory=%d%n", result._1, result._2);
	}
	
	private static final Tuple<Long,Long> runATest(File shpFile, int count) throws Exception {
		load(shpFile);
		
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(count);
		List<Long> memUsage = Lists.newArrayListWithExpectedSize(count);
		for ( int i =0; i < count; ++i ) {
			Tuple3<List<SimpleFeature>, StopWatch, Long> result = load(shpFile);
			
			elapseds.add(result._2.getElapsedInMillis());
			memUsage.add(result._3);
		}
		return Tuple.of(Globals.calcMean(elapseds), Globals.calcMean(memUsage));
	}
	
	private static final Tuple3<List<SimpleFeature>, StopWatch, Long>
	load(File shpFile) throws Exception {
		System.gc();
		long prevMem = Runtime.getRuntime().freeMemory();
		
		StopWatch watch = StopWatch.start();
		Shapefile shp = Shapefile.of(shpFile);
		List<SimpleFeature> featureList = shp.streamFeatures().toList();
		watch.stop();
		
		System.gc();
		long afterMem = Runtime.getRuntime().freeMemory();
		long memUsed = prevMem - afterMem;
		
		return Tuple.of(featureList, watch, memUsed);
	}
}
