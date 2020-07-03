package shpdiff;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;

import marmot.RecordSchema;
import marmot.type.DataType;
import record.shp.ShapefileDataSets;
import utils.func.FOption;
import utils.func.Tuple;
import utils.func.Tuple3;
import utils.geo.GeometryUtils;
import utils.geo.Shapefile;
import utils.geo.quadtree.point.PointQuadTree;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestCorrectness {
	private static final int TEST_COUNT = 100;
	private static final double DELETE_RATIO = 0.02;
	private static final double UPDATE_RATIO = 0.05;
	private static final double INSERT_RATIO = 0.03;
	private static final Charset EUC_KR = Charset.forName("euc-kr");
	private static final String SRID = "EPSG:5186";
	private static final File OUTPUT_OLD = new File("/home/kwlee/tmp/test_old");
	private static final File OUTPUT_NEW = new File("/home/kwlee/tmp/test_new");
	
	public static final void main(String... args) throws Exception {
		List<SimpleFeature> features = createTestShapefile(Globals.SMALL, OUTPUT_OLD);
		for ( int i =0; i < TEST_COUNT; ++i ) {
			runATest(i, features);
		}
	}
	
	private static void runATest(int idx, List<SimpleFeature> features) throws IOException {
		List<SimpleFeature> remains = features;
		List<SimpleFeature> updateds = Lists.newArrayList();
		List<SimpleFeature> inserteds = Lists.newArrayList();
		int[] answerDeletedIdxes = new int[0];
		int[] answerUpdatedIdxes = new int[0];
		int[] answerInsertedIndexes = new int[0];
		
		Tuple3<int[],int[],int[]> t = createChangedShapefile(OUTPUT_NEW, features);
		answerDeletedIdxes = t._1;
		answerUpdatedIdxes = t._2;
		answerInsertedIndexes = t._3;
		
		// Shapefile 변경 검출 모듈을 생성한다.
		ShapefileCompare cmp = new ShapefileCompare(new File(OUTPUT_OLD, "test.shp"),
													new File(OUTPUT_NEW, "test.shp"));
		cmp.run();
		
		int[] foundDeletedIdxes = cmp.getDeletedFeatureSeqNo().toArray();
		int[] foundUpdatedIdxes = cmp.getUpdateMappings().map(t2 -> t2._1).toIntFStream().toArray();
		int[] foundInsertedIdxes = cmp.getInsertedFeatures()
										.mapToInt(f -> (Integer)f.getAttribute(1))
										.toArray();
		boolean matchDeleteds = Arrays.equals(answerDeletedIdxes, foundDeletedIdxes);
		boolean matchUpdateds = Arrays.equals(answerUpdatedIdxes, foundUpdatedIdxes);
		boolean matchInserteds = Arrays.equals(answerInsertedIndexes, foundInsertedIdxes);
		System.out.printf("[%3d]: deleteds=%s, updateds=%s, inserteds=%s%n",
							idx, matchDeleteds, matchUpdateds, matchInserteds);
//		System.out.printf("%d,%d,%d,%d,%d,%d%n",
//							answerDeletedIdxes.length, answerUpdatedIdxes.length, answerInsertedIndexes.length,
//							foundDeletedIdxes.length, foundUpdatedIdxes.length, foundInsertedIdxes.length);
	}
	
	private static List<SimpleFeature> createTestShapefile(File orgFile, File srcFile)
		throws IOException {
		RecordSchema schema = RecordSchema.builder()
										.addColumn("the_geom", DataType.MULTI_LINESTRING)
										.addColumn("id", DataType.INT)
										.addColumn("name", DataType.STRING)
										.build();
		SimpleFeatureType sfType = ShapefileDataSets.toSimpleFeatureType("test", SRID, schema);

		Shapefile shp = Shapefile.of(orgFile, EUC_KR);
		
		GeomInfoQuadTree qtree = new GeomInfoQuadTree(shp.getTopBounds());
		List<SimpleFeature> features
						= shp.streamGeometries()
							.map(geom -> new GeomInfo(geom, 0))
							.filter(info -> {
								Envelope envl = new Envelope(info.center());
								envl.expandBy(0.01);
								return !qtree.query(envl).exists();
							})
							.peek(info -> qtree.insert(new GeomInfoValue(info)))
							.map(GeomInfo::geometry)
							.zipWithIndex()
							.map(t -> {
								String name = String.format("%d", t._2);
								Object[] values = new Object[] {t._1, t._2, name};
								return SimpleFeatureBuilder.build(sfType, values, null);
							})
							.toList();

		// 변경된 SimpleFeature를 Shapefile로 저장한다.
		Shapefile.writeShapefile(OUTPUT_OLD, features, EUC_KR, FOption.empty(), FOption.empty());
		
		return features;
	}
	
	private static Tuple3<int[],int[],int[]>
	createChangedShapefile(File newFile, List<SimpleFeature> features) throws IOException {
		List<SimpleFeature> remains = features;
		List<SimpleFeature> updateds = Lists.newArrayList();
		List<SimpleFeature> inserteds = Lists.newArrayList();
		int[] answerDeletedIdxes = new int[0];
		int[] answerUpdatedIdxes = new int[0];
		int[] answerInsertedIndexes = new int[0];
		
		Tuple<int[], List<SimpleFeature>> tDeleted = delete(remains, DELETE_RATIO);
		answerDeletedIdxes = tDeleted._1;
		remains = tDeleted._2;
		
		Tuple3<int[], List<SimpleFeature>, List<SimpleFeature>> tUpdated = update(remains, UPDATE_RATIO);
		answerUpdatedIdxes = tUpdated._1;
		features = tUpdated._2;
		updateds = tUpdated._3;
		
		Tuple<int[], List<SimpleFeature>> tInserted = insert(remains, INSERT_RATIO);
		answerInsertedIndexes = tInserted._1;
		inserteds = tInserted._2;
		
		List<SimpleFeature> changeds = Lists.newArrayList(remains);
		changeds.addAll(updateds);
		changeds.addAll(inserteds);
		
		// 변경된 SimpleFeature를 Shapefile로 저장한다.
		Shapefile.writeShapefile(newFile, changeds, EUC_KR, FOption.empty(), FOption.empty());
		
		return Tuple.of(answerDeletedIdxes, answerUpdatedIdxes, answerInsertedIndexes);
	}
	
	private static Tuple<int[], List<SimpleFeature>>
	delete(List<SimpleFeature> features, double ratio) {
		List<SimpleFeature> deleteds = FStream.from(features).sample(ratio).toList();
		int[] deletedIdxes = toIndexes(deleteds);
		
		List<SimpleFeature> remains = Lists.newArrayList(features);
		for ( int i = deletedIdxes.length-1; i >= 0; --i ) {
			remains.remove(deletedIdxes[i]);
		}
		
		return Tuple.of(deletedIdxes, remains);
	}
	
	private static Tuple3<int[], List<SimpleFeature>, List<SimpleFeature>>
	update(List<SimpleFeature> features, double ratio) {
		List<Tuple<SimpleFeature,Integer>> tuples = FStream.from(features)
															.zipWithIndex()
															.sample(ratio)
															.toList();
		List<SimpleFeature> updateds = FStream.from(tuples)
												.map(Tuple::_1)
												.map(f -> duplicate(f))
												.toList();
		int[] updatedIdxes = toIndexes(FStream.from(updateds));
		int[] removeIdxes = FStream.from(tuples).mapToInt(Tuple::_2).toArray();
		
		List<SimpleFeature> remains = Lists.newArrayList(features);
		for ( int i = removeIdxes.length-1; i >= 0; --i ) {
			SimpleFeature removed = remains.remove(removeIdxes[i]);
//			System.out.printf("%d <-> %d%n", removed.getAttribute(1), updatedIdxes[i]);
		}
		
		for ( SimpleFeature feature: updateds ) {
			String v = (String)feature.getAttribute(2);
			feature.setAttribute(2, "##" + v);
		}

		return Tuple.of(updatedIdxes, remains, updateds);
	}
	
	private static Tuple<int[], List<SimpleFeature>>
	insert(List<SimpleFeature> features, double ratio) {
		List<SimpleFeature> inserteds = FStream.from(features).sample(ratio).toList();
		int[] insertedIdxes = toIndexes(inserteds);
		inserteds = FStream.from(inserteds)
							.map(f -> changeGeometry(duplicate(f)))
							.toList();

		return Tuple.of(insertedIdxes, inserteds);
	}
	
	private static int[] toIndexes(List<SimpleFeature> features) {
		return FStream.from(features)
						.mapToInt(f -> (Integer)f.getAttribute(1))
						.toArray();
	}
	
	private static int[] toIndexes(FStream<SimpleFeature> strm) {
		return strm.mapToInt(f -> (Integer)f.getAttribute(1))
					.toArray();
	}
	
	private static SimpleFeature changeGeometry(SimpleFeature feature) {
		MultiLineString mline = (MultiLineString)feature.getAttribute(0);
		LineString line = (LineString)mline.getGeometryN(0);
		Coordinate end = line.getEndPoint().getCoordinate();
		Coordinate end2 = new Coordinate(end.x + 20, end.y + 20);
		LineString line2 = GeometryUtils.toLineString(line.getStartPoint().getCoordinate(), end2);
		MultiLineString mline2 = GeometryUtils.toMultiLineString(line2);
		feature.setAttribute(0, mline2);
		String v = (String)feature.getAttribute(2);
		feature.setAttribute(2, "NEW:" + v);
		
		return feature;
	}
	
	private static SimpleFeature duplicate(SimpleFeature feature) {
		SimpleFeatureType sfType = feature.getFeatureType();
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sfType);
		List<Object> values = feature.getAttributes();
		return builder.build(sfType, values, null);
	}
	
	private static class GeomInfoQuadTree extends PointQuadTree<GeomInfoValue, GeomInfoPartition> {
		public GeomInfoQuadTree(Envelope rootEnvl) {
			super(rootEnvl, bounds -> new GeomInfoPartition());
		}
	}
}
