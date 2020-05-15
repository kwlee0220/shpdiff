package shpdiff;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import utils.geo.SerializableUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeomInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private Geometry m_geom;
	private Envelope m_envl;
	private Coordinate m_center;
	private int m_seqno;
	
	GeomInfo(Geometry geom, int seqno) {
		m_geom = geom;
		m_envl = geom.getEnvelopeInternal();
		m_center = m_envl.centre();
		m_seqno = seqno;
	}
	
	public Geometry geometry() {
		return m_geom;
	}
	
	public Envelope envelope() {
		return m_envl;
	}
	
	public Coordinate center() {
		return m_center;
	}
	
	public int seqno() {
		return m_seqno;
	}
	
	@Override
	public String toString() {
		return String.format("%d:%s", m_seqno, m_center);
	}
	
	public static GeomInfo read(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		
		try {
			Geometry geom = SerializableUtils.readGeometry(ois);
			int seqno = ois.readInt();
			return new GeomInfo(geom, seqno);
		}
		catch ( ParseException e ) {
			throw new IOException("Geometry is corrupted: " + e);
		}
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		
		SerializableUtils.writeGeometry(oos, m_geom);
		oos.writeInt(m_seqno);
	}
	
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		
		try {
			m_geom = SerializableUtils.readGeometry(ois);
			m_envl = m_geom.getEnvelopeInternal();
			m_center = m_envl.centre();
			m_seqno = ois.readInt();
		}
		catch ( ParseException e ) {
			throw new IOException("Geometry is corrupted: " + e);
		}
	}
}
