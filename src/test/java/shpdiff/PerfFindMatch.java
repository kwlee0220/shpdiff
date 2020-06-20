package shpdiff;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryType;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import utils.StopWatch;
import utils.Utilities;
import utils.geo.Shapefile;
import utils.geo.quadtree.point.PointQuadTree;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfFindMatch {
	private static List<SimpleFeature> s_featureList;
	
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
		s_featureList = shp.streamFeatures().toList();
		shp.close();
		GeomInfoQuadTree qtree = buildQuadTree(mbr, s_featureList);
		
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(count);
		for ( int i =0; i < count; ++i ) {
			long elapsed = findMatches(s_featureList, qtree);
			elapseds.add(elapsed);
		}
		
		return Globals.calcMeanElapsed(elapseds);
	}
	
	private static long findMatches(List<SimpleFeature> features, GeomInfoQuadTree qtree) {
		StopWatch watch = StopWatch.start();
		for ( int i =0; i < features.size(); ++i ) {
			SimpleFeature feature = features.get(i);
			findUpdateInfo(i, feature, qtree);
		}
		watch.stop();
		return watch.getElapsedInMillis();
	}
	
	private static GeomInfoQuadTree buildQuadTree(Envelope mbr, List<SimpleFeature> features)
		throws IOException {
		GeomInfoQuadTree qtree = new GeomInfoQuadTree(mbr);
		for ( int i =0; i < features.size(); ++i ) {
			SimpleFeature feature = features.get(i);
			Geometry geom = (Geometry)feature.getAttribute("the_geom");
			GeomInfo geomInfo = new GeomInfo(geom, i);
			
			qtree.insert(new GeomInfoValue(geomInfo));
		}
		
		return qtree;
	}
	
	private static class GeomInfoQuadTree extends PointQuadTree<GeomInfoValue, GeomInfoPartition> {
		public GeomInfoQuadTree(Envelope rootEnvl) {
			super(rootEnvl, bounds -> new GeomInfoPartition());
		}
	}

	private static class GeomMatch {
		private GeomInfo m_oldInfo;
		private GeomInfo m_newInfo;
		private double m_diff;
		
		GeomMatch(GeomInfo oldInfo, GeomInfo newInfo) {
			m_oldInfo = oldInfo;
			m_newInfo = newInfo;
			m_diff = m_oldInfo.geometry().symDifference(m_newInfo.geometry()).getArea();
		}
		
		@Override
		public String toString() {
			return String.format("match: old=%d, new=%d, diff=%.3f",
									m_oldInfo.seqno(), m_newInfo.seqno(), m_diff);
		}
	}
	
	private static void findUpdateInfo(int seqno, SimpleFeature sf, GeomInfoQuadTree qtree) {
		GeomInfo geomInfo = new GeomInfo((Geometry)sf.getAttribute("the_geom"), seqno);
		List<GeomMatch> geomMatches = matchGeometry(geomInfo, qtree).toList();
		
		List<SimpleFeature> matches = FStream.from(geomMatches)
											.map(match -> s_featureList.get(match.m_oldInfo.seqno()))
											.filter(old -> equalsAttributes(sf, old))
											.toList();
		Utilities.checkState(matches.size() == 1);
	}
	
	private static final double EPSILON_DIST = 0.01;
	private static FStream<GeomMatch> matchGeometry(GeomInfo info, GeomInfoQuadTree qtree) {
		Envelope key = new Envelope(info.center());
		key.expandBy(EPSILON_DIST);
		
		return qtree.query(key)
					.map(found -> new GeomMatch(found.getGeomInfo(), info))
					.filter(m -> Double.compare(m.m_diff, 1) <= 0)
					.sort((m1,m2) -> Double.compare(m1.m_diff, m2.m_diff));
	}
	
	private static boolean equalsAttributes(SimpleFeature oldSf, SimpleFeature newSf) {
		return FStream.from(oldSf.getFeatureType().getAttributeDescriptors())
						.filter(desc -> !(desc.getType() instanceof GeometryType))
						.map(AttributeDescriptor::getName)
						.filter(name -> {
							Object oldV = oldSf.getAttribute(name);
							Object newV = newSf.getAttribute(name);
							return !oldV.equals(newV);
						})
						.findFirst().isAbsent();
	}
}
