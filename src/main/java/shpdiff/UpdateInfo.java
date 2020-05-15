package shpdiff;

import javax.annotation.Nullable;

import org.opengis.feature.simple.SimpleFeature;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UpdateInfo {
	private static final byte STATUS_UNCHANGED = 0;
	private static final byte STATUS_UPDATED = 1;
	private static final byte STATUS_DELETED = 2;
	private static final byte STATUS_INSERTED = 3;
	private static final UpdateInfo UNCHANGED = new UpdateInfo(STATUS_UNCHANGED, null);
	
	private byte m_status;
	@Nullable private SimpleFeature m_feature;
	
	private UpdateInfo(byte status, SimpleFeature feature) {
		m_status = status;
		m_feature = feature;
	}
	
	public static UpdateInfo unchanged() {
		return new UpdateInfo(STATUS_UNCHANGED, null);
	}
	
	public static UpdateInfo unchanged(SimpleFeature feature) {
		return new UpdateInfo(STATUS_UNCHANGED, feature);
	}
	
	public static UpdateInfo deleted(SimpleFeature feature) {
		return new UpdateInfo(STATUS_DELETED, feature);
	}
	
	public static UpdateInfo inserted() {
		return new UpdateInfo(STATUS_INSERTED, null);
	}
	
	public static UpdateInfo inserted(SimpleFeature feature) {
		return new UpdateInfo(STATUS_INSERTED, feature);
	}
	
	public static UpdateInfo updated(SimpleFeature feature) {
		return new UpdateInfo(STATUS_UPDATED, feature);
	}
	
	public boolean isUpdated() {
		return m_status == STATUS_UPDATED;
	}
	
	public boolean isUnchanged() {
		return m_status == STATUS_UNCHANGED;
	}
	
	public void setUnchanged() {
		m_status = STATUS_UNCHANGED;
	}
	
	public boolean isDeleted() {
		return m_status == STATUS_DELETED;
	}
	
	public boolean isInserted() {
		return m_status == STATUS_INSERTED;
	}
	
	public SimpleFeature feature() {
		return m_feature;
	}
	
	public void feature(SimpleFeature feature) {
		m_feature = feature;
	}
	
	@Override
	public String toString() {
		String statusStr = "unknown";
		switch ( m_status ) {
			case STATUS_UNCHANGED:
				statusStr = "unchanged";
				break;
			case STATUS_UPDATED:
				statusStr = "updated";
				break;
			case STATUS_DELETED:
				statusStr = "deleted";
				break;
			case STATUS_INSERTED:
				statusStr = "inserted";
				break;
			default:
				throw new AssertionError();
		}
		
		return statusStr;
	}
}
