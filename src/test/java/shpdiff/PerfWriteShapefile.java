package shpdiff;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.Lists;

import utils.StopWatch;
import utils.record.Record;
import utils.record.RecordSet;
import utils.record.geotools.SimpleFeatures;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfWriteShapefile {
	private static final int COUNT = 10;
	private static final File OUTPUT = new File("C:/Temp/output");
	
	public static final void main(String... args) throws Exception {
		long elapsed;
		
		runATest(Globals.SMALL, 10);
		System.out.println("SMALL: -----------------------------------------");
		
		runATest(Globals.MIDIUM, 10);
		System.out.println("MEDIUM: -----------------------------------------");
		
		runATest(Globals.LARGE, 10);
		System.out.println("LARGE: -----------------------------------------");
	}
	
	private static final void runATest(File shpFile, int count) throws Exception {
		List<Record> recordList = SimpleFeatures.readShapefile(shpFile, Charset.forName("euc-kr")).read().toList();
		
		runWithRatio(recordList, 0.01f);
		runWithRatio(recordList, 0.1f);
		runWithRatio(recordList, 0.3f);
		runWithRatio(recordList, 0.7f);;
		runWithRatio(recordList, 1f);
	}
	
	private static final long runWithRatio(List<Record> recordList, float ratio) throws Exception {
		List<Record> sampled = FStream.from(recordList).sample(recordList.size(), ratio).toList();
		
		write(sampled, OUTPUT);
		List<Long> elapseds = Lists.newArrayListWithExpectedSize(COUNT);
		for ( int i =0; i < COUNT; ++i ) {
			elapseds.add(write(sampled, OUTPUT));
		}
		elapseds.sort((v1,v2) -> Long.compare(v1, v2));
		elapseds.remove(COUNT-1);
		elapseds.remove(0);
		long avg = Math.round(FStream.from(elapseds).mapToLong(v -> (Long)v).average().get());
		
		System.out.printf("avg: ratio=%.0f, count=%d, elapsed=%s%n",
							ratio*100, sampled.size(), avg);
		
		return avg;
	}
	
	private static final long write(List<Record> recordList, File outDir) throws Exception {
		StopWatch watch = StopWatch.start();
		long count = SimpleFeatures.writeShapefile(outDir, "EPSG:4326")
									.setName("test")
									.write(RecordSet.from(recordList));
		long elapsed = watch.stopInMillis();
//		System.out.printf("\tcount=%d, elapsed=%s%n", count, elapsed);
		
		return elapsed;
	}
}
