package shpdiff;

import java.io.File;
import java.util.List;

import org.opengis.feature.simple.SimpleFeature;

import com.google.common.collect.Lists;

import utils.StopWatch;
import utils.func.Tuple;
import utils.geo.Shapefile;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfLoadTime {
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
		load(shpFile);
		
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(count);
		for ( int i =0; i < count; ++i ) {
			elapseds.add(load(shpFile)._2.getElapsedInMillis());
		}
		elapseds.sort((v1,v2) -> Long.compare(v1, v2));
		elapseds.remove(count-1);
		elapseds.remove(0);
		
		return Math.round(FStream.from(elapseds).mapToLong(v -> (Long)v).average().get());
	}
	
	private static final Tuple<List<SimpleFeature>, StopWatch> load(File shpFile) throws Exception {
		StopWatch watch = StopWatch.start();
		Shapefile shp = Shapefile.of(shpFile);
		List<SimpleFeature> featureList = shp.streamFeatures().toList();
		watch.stop();
		
		return Tuple.of(featureList, watch);
	}
}
