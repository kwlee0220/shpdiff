package shpdiff;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.feature.type.Name;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import utils.func.FOption;
import utils.func.Tuple;
import utils.geo.Shapefile;
import utils.geo.quadtree.point.PointQuadTree;
import utils.stream.FStream;
import utils.stream.IntFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileCompare {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileCompare.class);
	private static final double DEF_DIST_EPSILON = 0.01;
	
	private final Shapefile m_oldShpFile;
	private final Shapefile m_newShpFile;
	private final double m_epsilonDist;
	
	private Map<Integer,Integer> m_mappings;
	private UpdateInfo[] m_oldSfUpdateInfos;
	private UpdateInfo[] m_newSfUpdateInfos;
	
	public ShapefileCompare(File oldFile, File newFile) throws IOException {
		m_oldShpFile = Shapefile.of(oldFile);
		m_newShpFile = Shapefile.of(newFile);
		m_epsilonDist = DEF_DIST_EPSILON;
	}
	
	public void run() {
		try {
			m_oldSfUpdateInfos = loadOldShpFeatures();
			m_newSfUpdateInfos = new UpdateInfo[m_newShpFile.getRecordCount()];
			m_mappings = findUpdatedPairs();
		}
		catch ( IOException e ) {
			e.printStackTrace();
		}
	}
	
	public int getOldFeatureCount() {
		return m_oldSfUpdateInfos.length;
	}
	
	public int getNewFeatureCount() {
		return m_newSfUpdateInfos.length;
	}
	
	public IntFStream getDeletedFeatureSeqNo() {
		return FStream.of(m_oldSfUpdateInfos)
						.zipWithIndex()
						.filter(t -> t._1.isDeleted())
						.mapToInt(Tuple::_2);
	}
	
	public FStream<SimpleFeature> getDeletedFeatures() throws IOException {
		return FStream.of(m_oldSfUpdateInfos)
						.filter(UpdateInfo::isDeleted)
						.map(UpdateInfo::feature);
	}
	
	public FStream<Tuple<Integer,Integer>> getUpdateMappings() {
		return FStream.of(m_newSfUpdateInfos)
						.zipWithIndex()
						.filter(t -> t._1.isUpdated())
						.map(t -> Tuple.of(m_mappings.get(t._2), t._2));
	}
	
	public FStream<SimpleFeature> getUpdatedFeatures() throws IOException {
		return FStream.of(m_newSfUpdateInfos)
						.filter(UpdateInfo::isUpdated)
						.map(UpdateInfo::feature);
	}
	
	public IntFStream getInsertedFeatureSeqNo() {
		return FStream.of(m_newSfUpdateInfos)
						.zipWithIndex()
						.filter(t -> t._1.isInserted())
						.mapToInt(Tuple::_2);
	}
	
	public FStream<SimpleFeature> getInsertedFeatures() throws IOException {
		return FStream.of(m_newSfUpdateInfos)
						.filter(UpdateInfo::isInserted)
						.map(UpdateInfo::feature);
	}
	
	private UpdateInfo[] loadOldShpFeatures() throws IOException {
		UpdateInfo[] infos = new UpdateInfo[m_oldShpFile.getRecordCount()];
		m_oldShpFile.streamFeatures()
					.zipWithIndex()
					.forEach(t -> infos[t._2] = UpdateInfo.deleted(t._1));
		return infos;
	}
	
//	private Map<Integer,Integer> findGeometryMapping() throws IOException {
//		SimplePointQuadTree qtree = buildQuadTree(m_oldShpFile, m_oldSfUpdateInfos);
//
//		Map<Integer,Integer> mappings = Maps.newHashMap();
//		for ( int i =0; i < m_newSfUpdateInfos.length; ++i ) {
//			UpdateInfo info = m_newSfUpdateInfos[i];
//			GeomInfo geomInfo = new GeomInfo((Geometry)info.feature().getAttribute("the_geom"), i);
//			match(geomInfo, qtree)
//				.ifPresent(m -> {
//					mappings.put(m.m_newInfo.seqno(), m.m_oldInfo.seqno());
//				});
//		}
//		
//		return mappings;
//	}
//	
	private Map<Integer,Integer> findUpdatedPairs() throws IOException {
		GeomInfoQuadTree qtree = buildQuadTree(m_oldShpFile, m_oldSfUpdateInfos);
		
		return m_newShpFile.streamFeatures()
							.zipWithIndex()
							.flatMapOption(t -> findUpdateInfo(t._2, t._1, qtree))
							.toMap(Tuple::_2, Tuple::_1);
	}
	
	private FOption<Tuple<Integer,Integer>>
	findUpdateInfo(int seqno, SimpleFeature sf, GeomInfoQuadTree qtree) {
		GeomInfo geomInfo = new GeomInfo((Geometry)sf.getAttribute("the_geom"), seqno);
		List<GeomMatch> geomMatches = matchGeometry(geomInfo, qtree).toList();
		if ( geomMatches.size() == 1 ) {
			GeomMatch match = geomMatches.get(0);
			int oldSeqno = match.m_oldInfo.seqno();
			
			if ( matchAttributes(seqno, sf, match) ) {
				// 속성값까지 정확히 매칭된 경우
				m_newSfUpdateInfos[seqno] = UpdateInfo.unchanged();
				m_oldSfUpdateInfos[oldSeqno] = UpdateInfo.unchanged();
			}
			else {
				m_newSfUpdateInfos[seqno] = UpdateInfo.updated(sf);
				
				SimpleFeature oldSf = m_oldSfUpdateInfos[oldSeqno].feature();
				m_oldSfUpdateInfos[oldSeqno] = UpdateInfo.updated(oldSf);
			}
			return FOption.of(Tuple.of(oldSeqno, seqno));
		}
		else if ( geomMatches.size() > 1 ) {
			return FStream.from(geomMatches)
						.filter(geomMatch -> matchAttributes(seqno, sf, geomMatch))
						.map(m -> m.m_oldInfo)
						.filter(ginfo -> m_oldSfUpdateInfos[ginfo.seqno()].isDeleted())
						.findFirst()
						.map(oldInfo -> {
							int oldSeqno = oldInfo.seqno();
							m_newSfUpdateInfos[seqno] = UpdateInfo.unchanged();
							m_oldSfUpdateInfos[oldSeqno] = UpdateInfo.unchanged();
							return Tuple.of(oldSeqno, seqno);
						})
						.orElse(() -> {
							int oldSeqno = geomMatches.get(0).m_oldInfo.seqno();
							m_newSfUpdateInfos[seqno] = UpdateInfo.unchanged();
							m_oldSfUpdateInfos[oldSeqno] = UpdateInfo.unchanged();
							return FOption.of(Tuple.of(oldSeqno, seqno));
						});
		}
		else {
			m_newSfUpdateInfos[seqno] = UpdateInfo.inserted(sf);
			return FOption.empty();
		}
	}
	
	private boolean matchAttributes(int seqno, SimpleFeature sf, GeomMatch geomMatch) {
		int oldSeqno = geomMatch.m_oldInfo.seqno();
		UpdateInfo oldUpdateInfo = m_oldSfUpdateInfos[oldSeqno];
		if ( oldUpdateInfo.isDeleted() ) {
			SimpleFeature oldSf = m_oldSfUpdateInfos[oldSeqno].feature();
			
			String v = (String)sf.getAttribute(2);
//			if ( v.equals("##3") ) {
//				System.out.println("" + oldSf.getAttribute(2) + ", " + sf.getAttribute(2)
//									+ ", diff=" + findDifference(oldSf, sf));
//			}
			
			return findDifference(oldSf, sf).isAbsent();
		}
		else {
			return false;
		}
	}
	
	private FOption<String> findDifference(SimpleFeature oldSf, SimpleFeature newSf) {
		return FStream.from(oldSf.getFeatureType().getAttributeDescriptors())
						.filter(desc -> !(desc.getType() instanceof GeometryType))
						.map(AttributeDescriptor::getName)
						.findFirst(name -> {
							Object oldV = oldSf.getAttribute(name);
							Object newV = newSf.getAttribute(name);
							return !oldV.equals(newV);
						})
						.map(Name::getLocalPart);
	}
	
	private static GeomInfoQuadTree buildQuadTree(Shapefile shp, UpdateInfo[] infos)
		throws IOException {
		GeomInfoQuadTree qtree = new GeomInfoQuadTree(shp.getTopBounds());
		for ( int i =0; i < infos.length; ++i ) {
			UpdateInfo info = infos[i];
			
			GeomInfo geomInfo = new GeomInfo((Geometry)info.feature().getAttribute("the_geom"), i);
			qtree.insert(new GeomInfoValue(geomInfo));
		}
		
		return qtree;
	}
	
	public static class GeomInfoQuadTree extends PointQuadTree<GeomInfoValue, GeomInfoPartition> {
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
			return String.format("%d<->%d:%.3f", m_oldInfo.seqno(), m_newInfo.seqno(), m_diff);
		}
	}
	
	private FStream<GeomMatch> matchGeometry(GeomInfo info, GeomInfoQuadTree qtree) {
		Envelope key = new Envelope(info.center());
		key.expandBy(m_epsilonDist);
		
		return qtree.query(key)
					.map(found -> new GeomMatch(found.getGeomInfo(), info))
					.filter(m -> Double.compare(m.m_diff, 1) <= 0)
					.sort((m1,m2) -> Double.compare(m1.m_diff, m2.m_diff));
		
//		return qtree.query(key)
//					.map(found -> new Match(found.getGeomInfo(), info))
//					.filter(m -> Double.compare(m.m_diff, 1) <= 0)
//					.takeTopK(1, (m1,m2) -> Double.compare(m1.m_diff, m2.m_diff))
//					.findFirst();
	}
}
