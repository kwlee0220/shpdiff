package shpdiff;

import static marmot.support.SerializableUtils.readEnvelope;
import static marmot.support.SerializableUtils.readList;
import static marmot.support.SerializableUtils.writeCollection;
import static marmot.support.SerializableUtils.writeCoordinate;
import static marmot.support.SerializableUtils.writeEmptyCollection;
import static marmot.support.SerializableUtils.writeEnvelope;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;

import utils.geo.quadtree.point.PointPartition;
import utils.geo.quadtree.point.PointValue;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class GeomInfoPartition implements PointPartition<GeomInfoValue>, Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger s_logger = LoggerFactory.getLogger(GeomInfoPartition.class);
	private static final int MAX_MINI_PARTITION_SLOTS = 64;
	
	private Envelope m_valueBounds;
	private List<PointValue> m_slots;
	private int m_ptrCount;
	private boolean m_grouped;
	
	public GeomInfoPartition() {
		m_valueBounds = new Envelope();
		m_slots = Lists.newArrayList();
		m_ptrCount = 0;
		m_grouped = false;
	}
	
	private GeomInfoPartition(Envelope dataBounds, int count, List<GeomInfoValue> ptrs,
									List<GeomInfoValueGroup> grps) {
		m_valueBounds = dataBounds;
		m_slots = Lists.newArrayListWithExpectedSize(ptrs.size() + grps.size());
		m_slots.addAll(grps);
		m_slots.addAll(ptrs);
		m_ptrCount = count;
		m_grouped = grps.size() > 0;
	}
	
	public int getMaxSlots() {
		return MAX_MINI_PARTITION_SLOTS;
	}

	@Override
	public Envelope getBounds() {
		return m_valueBounds;
	}
	
	@Override
	public boolean add(GeomInfoValue value) {
		return add(value, true);
	}
	
	@Override
	public boolean add(GeomInfoValue value, boolean reserveForSpeed) {
		if ( m_slots.size() < MAX_MINI_PARTITION_SLOTS ) {
			m_valueBounds.expandToInclude(value.getEnvelope());
			m_slots.add(value);
			++m_ptrCount;
			
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public boolean expand() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return m_ptrCount;
	}

	@Override
	public FStream<GeomInfoValue> values() {
		if ( m_grouped ) {
			return FStream.from(m_slots)
							.flatMap(slot -> {
								if ( slot instanceof GeomInfoValueGroup ) {
									return ((GeomInfoValueGroup)slot).stream();
								}
								else {
									return FStream.of((GeomInfoValue)slot);
								}
							});
		}
		else {
			return FStream.from(m_slots).map(o -> (GeomInfoValue)o);
		}
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		
		writeEnvelope(oos, m_valueBounds);
		oos.writeInt(m_ptrCount);
		
		if ( !m_grouped ) {
			writeCollection(oos, m_slots, (os, v) -> writeCoordinate(os, v.getCoordinate()));
			writeEmptyCollection(oos);
		}
		else {
			List<GeomInfoValue> ptrs = new ArrayList<>();
			List<GeomInfoValueGroup> grps = new ArrayList<>();
			
			for ( PointValue slot: m_slots ) {
				if ( slot instanceof GeomInfoValue ) {
					ptrs.add((GeomInfoValue)slot);
				}
				else {
					grps.add((GeomInfoValueGroup)slot);
				}
			}
			writeCollection(oos, ptrs, (os, v) -> os.writeObject(v));
			writeCollection(oos, grps, (os, v) -> os.writeObject(v));
		}
	}
	
	public static GeomInfoPartition read(ObjectInputStream ois)
		throws IOException, ClassNotFoundException {
		Envelope envl = readEnvelope(ois);
		int count = ois.readInt();
		List<GeomInfoValue> ptrs = readList(ois, GeomInfoValue::read);
		List<GeomInfoValueGroup> grps = readList(ois, GeomInfoValueGroup::read);
		
		return new GeomInfoPartition(envl, count, ptrs, grps);
	}
	
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		
		m_valueBounds = readEnvelope(ois);
		m_ptrCount = ois.readInt();
		
		List<GeomInfoValue> ptrs = readList(ois, GeomInfoValue::read);
		List<GeomInfoValueGroup> grps = readList(ois, GeomInfoValueGroup::read);

		m_grouped = grps.size() > 0;
		m_slots = Lists.newArrayListWithExpectedSize(ptrs.size() + grps.size());
		m_slots.addAll(grps);
		m_slots.addAll(ptrs);
	}
	
	@Override
	public String toString() {
		return String.format("%s(%d/%d,%.1f%%)", getClass().getSimpleName(),
								m_ptrCount, getMaxSlots(), 	(double)m_ptrCount/getMaxSlots()*100);
	}
}