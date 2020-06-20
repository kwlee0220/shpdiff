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
	
	public static final File SMALL = new File("C:/Temp/지하매설물 과제/성능평가용 데이터/철도중심선_2013 (소형)/link_info.shp");
	public static final File MIDIUM = new File("C:/Temp/지하매설물 과제/성능평가용 데이터/하수관 (중형)/SWL_PIPE_LM.shp");
	public static final File LARGE = new File("C:/Temp/지하매설물 과제/성능평가용 데이터/도로망_링크_2013 (대형)/link_info.shp");
	public static final File[] DATA_FILES = new File[] {SMALL, MIDIUM, LARGE};
	
	public static long calcMeanElapsed(List<Long> elapsedList) {
		elapsedList.sort((v1,v2) -> Long.compare(v1, v2));
		elapsedList.remove(elapsedList.size()-1);
		elapsedList.remove(0);
		
		return Math.round(FStream.from(elapsedList).mapToLong(v -> (Long)v).average().get());
	}
}
