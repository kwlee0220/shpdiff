package shpdiff;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.opengis.feature.simple.SimpleFeature;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import utils.StopWatch;
import utils.func.Tuple;
import utils.geo.Shapefile;
import utils.geo.quadtree.point.PointQuadTree;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfLoadQTree {
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
		Shapefile shp = Shapefile.of(shpFile);
		Envelope mbr = shp.getTopBounds();
		List<SimpleFeature> featureList = shp.streamFeatures().toList();
		shp.close();
		
		buildQuadTree(mbr, featureList);
		
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(count);
		List<Long> memUsage = Lists.newArrayListWithExpectedSize(count);
		for ( int i =0; i < count; ++i ) {
			Tuple<Long,Long> result = buildQuadTree(mbr, featureList);
			elapseds.add(result._1);
			memUsage.add(result._2);
		}
		
		return Tuple.of(Globals.calcMean(elapseds), Globals.calcMean(memUsage));
	}
	
	private static Tuple<Long,Long> buildQuadTree(Envelope mbr, List<SimpleFeature> features)
		throws IOException {
		System.gc();
		long prevMem = Runtime.getRuntime().freeMemory();
		
		StopWatch watch = StopWatch.start();
		GeomInfoQuadTree qtree = new GeomInfoQuadTree(mbr);
		for ( int i =0; i < features.size(); ++i ) {
			SimpleFeature feature = features.get(i);
			Geometry geom = (Geometry)feature.getAttribute("the_geom");
			GeomInfo geomInfo = new GeomInfo(geom, i);
			
			qtree.insert(new GeomInfoValue(geomInfo));
		}
		watch.stop();
		System.gc();
		long afterMem = Runtime.getRuntime().freeMemory();
		long memUsed = prevMem - afterMem;
		
		System.out.println("depth=" + qtree.getDepth()
							+ ", leaf-node count=" + qtree.streamLeafNodes().count());
		
		return Tuple.of(watch.getElapsedInMillis(), memUsed);
	}
	
	private static class GeomInfoQuadTree extends PointQuadTree<GeomInfoValue, GeomInfoPartition> {
		public GeomInfoQuadTree(Envelope rootEnvl) {
			super(rootEnvl, bounds -> new GeomInfoPartition());
		}
	}
}
