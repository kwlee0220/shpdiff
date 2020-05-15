package shpdiff;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

import utils.geo.quadtree.point.PointValue;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeomInfoValue implements PointValue, Comparable<GeomInfoValue>, Serializable {
	private static final long serialVersionUID = 1L;
	
	private GeomInfo m_info;
	
	public GeomInfoValue(GeomInfo geomInfo) {
		m_info = geomInfo;
	}

	@Override
	public Coordinate getCoordinate() {
		return m_info.center();
	}

	@Override
	public Envelope getEnvelope() {
		return m_info.envelope();
	}
	
	public GeomInfo getGeomInfo() {
		return m_info;
	}
	
	@Override
	public String toString() {
		return m_info.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof GeomInfoValue) ) {
			return false;
		}
		
		GeomInfoValue other = (GeomInfoValue)obj;
		return m_info.seqno() == other.m_info.seqno();
	}
	
	@Override
	public int hashCode() {
		return m_info.seqno();
	}

	@Override
	public int compareTo(GeomInfoValue o) {
		return m_info.seqno() - o.m_info.seqno();
	}
	
	public static GeomInfoValue read(ObjectInputStream ois) throws IOException,
																	ClassNotFoundException {
		return new GeomInfoValue(GeomInfo.read(ois));
	}
	
	private void writeObject(ObjectOutputStream ois) throws IOException {
		ois.defaultWriteObject();
		
		ois.writeObject(m_info);
	}
	
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		
		m_info = (GeomInfo)ois.readObject();
	}
}