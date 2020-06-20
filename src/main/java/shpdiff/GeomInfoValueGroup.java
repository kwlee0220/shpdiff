package shpdiff;

import static utils.record.SerializableUtils.readList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

import utils.geo.quadtree.point.PointValue;
import utils.record.SerializableUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class GeomInfoValueGroup implements PointValue, Serializable {
	private static final long serialVersionUID = 1L;
	
	private List<GeomInfoValue> m_infos;
	
	GeomInfoValueGroup(List<GeomInfoValue> infos) {
		m_infos = infos;
	}

	@Override
	public Coordinate getCoordinate() {
		return m_infos.get(0).getCoordinate();
	}

	@Override
	public Envelope getEnvelope() {
		return m_infos.get(0).getEnvelope();
	}
	
	FStream<GeomInfoValue> stream() {
		return FStream.from(m_infos);
	}
	
	public static GeomInfoValueGroup read(ObjectInputStream ois) throws IOException,
																			ClassNotFoundException {
		List<GeomInfoValue> infoList = readList(ois, GeomInfoValue::read);
		return new GeomInfoValueGroup(infoList);
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		
		SerializableUtils.writeCollection(oos, m_infos, (os,v) -> os.writeObject(v));
	}
	
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		
		m_infos = readList(ois, GeomInfoValue::read);
	}
}