package shpdiff;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;

import utils.func.FOption;
import utils.func.Tuple;
import utils.func.Tuple3;
import utils.func.Tuple4;
import utils.geo.GeometryUtils;
import utils.geo.Shapefile;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestChanged {
	private static final int COUNT = 10;
	private static final double DELETE_RATIO = 0.02;
	private static final double UPDATE_RATIO = 0.05;
	private static final double INSERT_RATIO = 0.03;
	private static final Charset EUC_KR = Charset.forName("euc-kr");
	private static final File OUTPUT = new File("/home/kwlee/tmp/output");
	private static final File AVRO_FILE = new File("/home/kwlee/tmp/output.avro");
	
	public static final void main(String... args) throws Exception {
		long elapsed;
		
		Shapefile shp = Shapefile.of(Globals.SMALL, EUC_KR);
		String srid = "EPSG:5186";
		List<SimpleFeature> features = shp.streamFeatures().toList();
		
		// 변경된 SimpleFeature 세트를 생성한다.
		Tuple4<int[], int[], int[], List<SimpleFeature>> t4 = makeChanges(features);
		
		// 변경된 SimpleFeature를 Shapefile로 저장한다.
		Shapefile.writeShapefile(OUTPUT, t4._4, EUC_KR, FOption.empty(), FOption.empty());
		
		// Shapefile 변경 검출 모듈을 생성한다.
		ShapefileCompare cmp = new ShapefileCompare(Globals.SMALL, new File(OUTPUT, "link_info.shp"));
		cmp.run();
		
		int[] deleteds = cmp.getDeletedFeatureSeqNo().toArray();
		int[] updateds = cmp.getUpdateMappings().map(t2 -> t2._1).toIntFStream().toArray();
		int[] inserteds = cmp.getInsertedFeatures().map(f -> (Double)f.getAttribute(1))
								.mapToInt(v -> v.intValue()-1024).toArray();
		System.out.println("inserteds?: " + Arrays.equals(t4._1, inserteds));
		System.out.println("deleteds?: " + Arrays.equals(t4._2, deleteds));
		System.out.println("updateds?: " + Arrays.equals(t4._3, updateds));
	}
	
	private static Tuple4<int[], int[], int[], List<SimpleFeature>>
	makeChanges(List<SimpleFeature> features) {
		Tuple<int[], List<SimpleFeature>> t = delete(features, DELETE_RATIO);
		int[] deletedIndexes = t._1;
		List<SimpleFeature> remains = t._2;
		
		Tuple3<int[], List<SimpleFeature>, List<SimpleFeature>> t2 = update(remains, UPDATE_RATIO);
		int[] updatedIndexes = t2._1;
		remains = t2._2;
		List<SimpleFeature> updatedFeatures = t2._3;
		
		Tuple<int[], List<SimpleFeature>> t3 = insert(remains, INSERT_RATIO);
		int[] insertedIndexes = t3._1;
		List<SimpleFeature> insertedFeatures = t3._2;
		
		List<SimpleFeature> changeds = Lists.newArrayList(remains);
		changeds.addAll(updatedFeatures);
		changeds.addAll(insertedFeatures);
		
		return Tuple.of(insertedIndexes, deletedIndexes, updatedIndexes, changeds);
	}
	
	private static Tuple<int[], List<SimpleFeature>>
	delete(List<SimpleFeature> srcFeatures, double ratio) {
		List<SimpleFeature> deleteds = FStream.from(srcFeatures).sample(ratio).toList();
		int[] deletedIdxes = toIndexes(deleteds);
		
		for ( int i = deletedIdxes.length-1; i >= 0; --i ) {
			srcFeatures.remove(deletedIdxes[i]);
		}
		
		return Tuple.of(deletedIdxes, srcFeatures);
	}
	
	private static Tuple3<int[], List<SimpleFeature>, List<SimpleFeature>>
	update(List<SimpleFeature> remains, double ratio) {
		List<Tuple<SimpleFeature,Integer>> tuples = FStream.from(remains)
															.zipWithIndex()
															.sample(ratio)
															.toList();
		List<SimpleFeature> updateds = FStream.from(tuples).map(Tuple::_1).toList();
		int[] updatedIdxes = toIndexes(FStream.from(updateds));
		int[] removeIdxes = FStream.from(tuples).mapToInt(t -> t._2).toArray();
		
		for ( int i = removeIdxes.length-1; i >= 0; --i ) {
			SimpleFeature removed = remains.remove(removeIdxes[i]);
//			System.out.printf("%.0f <-> %d%n", removed.getAttribute(1), updatedIdxes[i]);
		}
		
		for ( SimpleFeature feature: updateds ) {
			double v = (Double)feature.getAttribute(1);
			feature.setAttribute(1, -v);
		}

		return Tuple.of(updatedIdxes, remains, updateds);
	}
	
	private static Tuple<int[], List<SimpleFeature>>
	insert(List<SimpleFeature> remains, double ratio) {
		List<SimpleFeature> inserteds = FStream.from(remains).sample(ratio).toList();
		int[] insertedIdxes = toIndexes(inserteds);
		FStream.from(inserteds).forEach(f -> changeGeometry(f));

		return Tuple.of(insertedIdxes, inserteds);
	}
	
	private static int[] toIndexes(List<SimpleFeature> features) {
		return FStream.from(features)
						.mapToInt(f -> ((Double)f.getAttribute(1)).intValue())
						.toArray();
	}
	
	private static int[] toIndexes(FStream<SimpleFeature> strm) {
		return strm.mapToInt(f -> ((Double)f.getAttribute(1)).intValue())
					.toArray();
	}
	
	private static void changeGeometry(SimpleFeature feature) {
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(feature.getFeatureType());
		SimpleFeature copied = builder.build(feature.getFeatureType(), feature.getAttributes(), null);
		
		MultiLineString mline = (MultiLineString)copied.getAttribute(0);
		LineString line = (LineString)mline.getGeometryN(0);
		Coordinate end = line.getEndPoint().getCoordinate();
		Coordinate end2 = new Coordinate(end.x + 20, end.y + 20);
		LineString line2 = GeometryUtils.toLineString(line.getStartPoint().getCoordinate(), end2);
		MultiLineString mline2 = GeometryUtils.toMultiLineString(line2);
		copied.setAttribute(0, mline2);
		double v = (Double)copied.getAttribute(1);
		copied.setAttribute(1, v + 1024);
	}
}
