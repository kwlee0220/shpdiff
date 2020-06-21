package shpdiff;

import java.io.File;
import java.util.List;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Globals {
	private Globals() {
		throw new AssertionError("Should not be called: class=" + getClass());
	}
	
//	public static final File SMALL = new File("/home/kwlee/tmp/shpdiff_perf_test/railway_small/link_info.shp");
//	public static final File MIDIUM = new File("/home/kwlee/tmp/shpdiff_perf_test/sewage_medium/SWL_PIPE_LM.shp");
//	public static final File LARGE = new File("/home/kwlee/tmp/shpdiff_perf_test/road_large/link_info.shp");
	public static final File SMALL = new File("c:/temp/shpdiff_perf_test/railway_small/link_info.shp");
	public static final File MIDIUM = new File("c:/temp/shpdiff_perf_test/sewage_medium/SWL_PIPE_LM.shp");
	public static final File LARGE = new File("c:/temp/shpdiff_perf_test/road_large/link_info.shp");
	public static final File[] DATA_FILES = new File[] {SMALL, MIDIUM, LARGE};
	
	public static long calcMeanElapsed(List<Long> elapsedList) {
		elapsedList.sort((v1,v2) -> Long.compare(v1, v2));
		elapsedList.remove(elapsedList.size()-1);
		elapsedList.remove(0);
		
		return Math.round(FStream.from(elapsedList).mapToLong(v -> (Long)v).average().get());
	}
}
