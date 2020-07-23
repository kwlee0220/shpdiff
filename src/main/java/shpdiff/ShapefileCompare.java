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
	
	/**
	 * 두 shp 파일 사이의 변경 내용을 검출하기 위한 {@link ShapefileCompare} 객체를 생성한다.
	 * 
	 * @param oldFile	이전 shp 파일 객체
	 * @param newFile	새 shp 파일 객체
	 * @throws	IOException	shp 파일 적재시 예외가 발생한 경우
	 */
	public ShapefileCompare(File oldFile, File newFile) throws IOException {
		m_oldShpFile = Shapefile.of(oldFile);
		m_newShpFile = Shapefile.of(newFile);
		m_epsilonDist = DEF_DIST_EPSILON;
	}
	
	public void run() {
		try {
			// 이전 shp 파일에서 레코드를 읽어 삭제(STATUS_DELETED)로 태깅한다.
			m_oldSfUpdateInfos = loadOldShpFeatures();
			
			// 새 shp 파일에서 레코드를 읽어들인다.
			m_newSfUpdateInfos = new UpdateInfo[m_newShpFile.getRecordCount()];
			
			// 이전 shp 레코드들과 새 shp 레코드들의 공간 객체를 비교하여
			// 동일 객체를 갖는 레코드들 사이의 매핑 관계를 구한다.
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
		// 이전 shp 파일에서 레코드를 읽어 삭제(STATUS_DELETED)로 태깅한다.
		//
		UpdateInfo[] infos = new UpdateInfo[m_oldShpFile.getRecordCount()];
		m_oldShpFile.streamFeatures()
					.zipWithIndex()
					.forEach(t -> infos[t._2] = UpdateInfo.deleted(t._1));
		return infos;
	}
	
	private Map<Integer,Integer> findUpdatedPairs() throws IOException {
		GeomInfoQuadTree qtree = buildQuadTree(m_oldShpFile, m_oldSfUpdateInfos);
		
		return m_newShpFile.streamFeatures()
							.zipWithIndex()
							.flatMapOption(t -> findUpdateInfo(t._2, t._1, qtree))
							.toMap(Tuple::_2, Tuple::_1);
	}
	
	private FOption<Tuple<Integer,Integer>>
	findUpdateInfo(int seqno, SimpleFeature sf, GeomInfoQuadTree qtree) {
		// 새 shp 레코드와 매핑되는 이전 shp 레코드를 검색하여, 변경 여부를 검출한다.
		//
		GeomInfo geomInfo = new GeomInfo((Geometry)sf.getAttribute("the_geom"), seqno);
		
		// 새 shp 레코드('sf')의 공간잭체를 이용하여 quad-tree에서 검색한다.
		// 검색된 이전 shp 레코드들 중에서 '삭제'로  태깅된 것만 선택한다.
		List<GeomMatch> geomMatches = matchGeometry(geomInfo, qtree)
//										.filter(gm -> m_oldSfUpdateInfos[gm.m_oldInfo.seqno()].isDeleted())
//										.peek(gm -> System.out.println(m_oldSfUpdateInfos[gm.m_oldInfo.seqno()].status()))
										.toList();
		if ( geomMatches.size() == 1 ) {	// 검색된 이전 shp 레코드가 1개인 경우
			GeomMatch match = geomMatches.get(0);
			int oldSeqno = match.m_oldInfo.seqno();
			
			// 검색된 이전 레코드의 속성 값들과 새 shp 레코드의 속성 값을 비교한다.
			String diffCol = findDifferentColumn(seqno, sf, match);
			if ( diffCol == null ) {
				// 속성 값까지 정확히 매칭된 경우
				m_newSfUpdateInfos[seqno] = UpdateInfo.unchanged();
				m_oldSfUpdateInfos[oldSeqno] = UpdateInfo.unchanged();
			}
			else {
				s_logger.info("found difference: {} <-> {}, diff_col={}", oldSeqno, seqno, diffCol);
				
				// 속성 값들 중 일부가 다른 경우
				m_newSfUpdateInfos[seqno] = UpdateInfo.updated(sf);
				
				SimpleFeature oldSf = m_oldSfUpdateInfos[oldSeqno].feature();
				m_oldSfUpdateInfos[oldSeqno] = UpdateInfo.updated(oldSf);
			}
			return FOption.of(Tuple.of(oldSeqno, seqno));
		}
		else if ( geomMatches.size() > 1 ) {	// 검색된 이전 shp 레코드가 2개 이상인 경우
			if ( s_logger.isDebugEnabled() ) {
				s_logger.debug("multiple geometry matches: {}", geomMatches);
			}
			return FStream.from(geomMatches)
						// 검색된 이전 shp 레코드들 중 속성 값이 동일한 레코드만 뽑는다.
						.filter(geomMatch -> findDifferentColumn(seqno, sf, geomMatch) != null)
						.map(m -> m.m_oldInfo)
						// '삭제'로 태깅된 이전 shp 레코드만 뽑는다.
						.filter(ginfo -> m_oldSfUpdateInfos[ginfo.seqno()].isDeleted())
						// 첫번째 레코드를 뽑는다.
						.findFirst()
						.map(oldInfo -> {
							// 공간 객체도 동일하고, 속성 값도 모두 동일한
							// 기존 shp 레코드와 새 shp 레코드 쌍을 구한 경우
							int oldSeqno = oldInfo.seqno();
							m_newSfUpdateInfos[seqno] = UpdateInfo.unchanged();
							m_oldSfUpdateInfos[oldSeqno] = UpdateInfo.unchanged();
							
							return Tuple.of(oldSeqno, seqno);
						})
						.orElse(() -> {
							// 공간 객체는 동일하고, 속성 값도 모두 동일한 기존 shp 레코드는 없는 경우
							// quad-tree를 통해 검색된 이전 shp 레코드들 중 하나를 
							
							int oldSeqno = geomMatches.get(0).m_oldInfo.seqno();
							SimpleFeature oldSf = m_oldSfUpdateInfos[oldSeqno].feature();
							m_oldSfUpdateInfos[oldSeqno] = UpdateInfo.updated(oldSf);
							m_newSfUpdateInfos[seqno] = UpdateInfo.updated(sf);
							
							s_logger.info("multiple geometry match => updated: {} = {}", oldSeqno, seqno);
							
							return FOption.of(Tuple.of(oldSeqno, seqno));
						});
		}
		else {	// 검색된 이전 shp 레코드가 없는 경우
			m_newSfUpdateInfos[seqno] = UpdateInfo.inserted(sf);
			return FOption.empty();
		}
	}

	private String findDifferentColumn(int seqno, SimpleFeature sf, GeomMatch geomMatch) {
		int oldSeqno = geomMatch.m_oldInfo.seqno();
		UpdateInfo oldUpdateInfo = m_oldSfUpdateInfos[oldSeqno];
		if ( oldUpdateInfo.isDeleted() ) {
			SimpleFeature oldSf = m_oldSfUpdateInfos[oldSeqno].feature();
			return findDifference(oldSf, sf).getOrNull();
		}
		else {
			return null;
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
	
	private static GeomInfoQuadTree buildQuadTree(Shapefile shp, UpdateInfo[] infos) throws IOException {
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
			m_diff = m_oldInfo.geometry().symDifference(m_newInfo.geometry()).getLength();
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
					.filter(m -> Double.compare(m.m_diff, 0.1) <= 0)
					.sort((m1,m2) -> Double.compare(m1.m_diff, m2.m_diff));
	}
}
