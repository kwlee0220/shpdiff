package shpdiff;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.opengis.feature.simple.SimpleFeature;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import utils.StopWatch;
import utils.geo.Shapefile;
import utils.geo.quadtree.point.PointQuadTree;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfLoadQTree {
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
		Shapefile shp = Shapefile.of(shpFile);
		Envelope mbr = shp.getTopBounds();
		List<SimpleFeature> featureList = shp.streamFeatures().toList();
		shp.close();
		
		buildQuadTree(mbr, featureList);
		
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(count);
		for ( int i =0; i < count; ++i ) {
			long elapsed = buildQuadTree(mbr, featureList);
			elapseds.add(elapsed);
		}
		
		return Globals.calcMeanElapsed(elapseds);
	}
	
	private static long buildQuadTree(Envelope mbr, List<SimpleFeature> features)
		throws IOException {
		StopWatch watch = StopWatch.start();
		GeomInfoQuadTree qtree = new GeomInfoQuadTree(mbr);
		for ( int i =0; i < features.size(); ++i ) {
			SimpleFeature feature = features.get(i);
			Geometry geom = (Geometry)feature.getAttribute("the_geom");
			GeomInfo geomInfo = new GeomInfo(geom, i);
			
			qtree.insert(new GeomInfoValue(geomInfo));
		}
		watch.stop();
		
		return watch.getElapsedInMillis();
	}
	
	private static class GeomInfoQuadTree extends PointQuadTree<GeomInfoValue, GeomInfoPartition> {
		public GeomInfoQuadTree(Envelope rootEnvl) {
			super(rootEnvl, bounds -> new GeomInfoPartition());
		}
	}
}
