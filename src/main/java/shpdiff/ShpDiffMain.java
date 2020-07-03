package shpdiff;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.PropertyConfigurator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;
import utils.StopWatch;
import utils.UsageHelp;
import utils.func.FOption;
import utils.func.Try;
import utils.func.Tuple;
import utils.geo.Shapefile;
import utils.stream.FStream;
import utils.stream.IntFStream;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="shpdiff",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="find shapefile difference")
public class ShpDiffMain implements Runnable {
	private static final Logger s_logger = LoggerFactory.getLogger(ShpDiffMain.class);

	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="base_shp", index="0", description={"base shapefile path"})
	private String m_basePath;
	
	@Parameters(paramLabel="new_shp", index="1", description={"new shapefile path"})
	private String m_newPath;
	
	@Parameters(paramLabel="output", index="2", description={"output directory path"})
	private String m_outputPath;
	
	@Option(names={"-shp"}, description={"write differences to shapefile"})
	private boolean m_writeShp = false;
	
	@Option(names={"-v"}, description={"verbose"})
	private boolean m_verbose = false;
	
	@Option(names={"-f"}, description={"force"})
	private boolean m_force = false;
	
	public static final void main(String... args) throws Exception {
		configureLog4j();

		ShpDiffMain cmd = new ShpDiffMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, args);
	}
	
	@Override
	public void run() {
		StopWatch watch = StopWatch.start();
		
		try  {
			ShapefileCompare shpCmp = new ShapefileCompare(new File(m_basePath), new File(m_newPath));
			shpCmp.run();
			
			File outputDir = new File(m_outputPath);
			if ( m_force ) {
				Try.run(() -> FileUtils.forceDelete(outputDir));
			}
			FileUtils.forceMkdir(outputDir);
			
			int deletedCount = writeDeleteds(shpCmp);
			int updatedCount = writeUpdateds(shpCmp);
			int insertedCount = writeInserteds(shpCmp);
			int unchangedCount = shpCmp.getOldFeatureCount() - deletedCount - updatedCount;
			
			if ( m_verbose ) {
				System.out.printf("elapsed=%s: base_shp_count=%d, new_shp_count=%d, "
								+ "unchangeds=%d, updateds=%d, inserteds=%d, deleteds=%d%n",
									watch.stopAndGetElpasedTimeString(),
									shpCmp.getOldFeatureCount(), shpCmp.getNewFeatureCount(),
									unchangedCount, updatedCount, insertedCount, deletedCount);
			}
		}
		catch ( IOException e ) {
			System.err.printf("failed: %s%n%n", e);
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
		finally {
		}
	}
	
	private int writeDeleteds(ShapefileCompare shpCmp) throws IOException {
		int[] deletedIdxes = shpCmp.getDeletedFeatureSeqNo().toArray();
		if ( deletedIdxes.length > 0 ) {
			File deletedsFile = new File(m_outputPath, "deleteds");
			try ( PrintWriter pw = new PrintWriter(new FileWriter(deletedsFile)) ) {
				IntFStream.of(deletedIdxes).forEach(pw::println);
			}
			
			if ( m_writeShp ) {
				List<SimpleFeature> features = shpCmp.getDeletedFeatures().toList();
				File deletedsDir = new File(m_outputPath, "deleteds_shp");
				Shapefile.writeShapefile(deletedsDir, features, Charset.defaultCharset(),
										FOption.empty(), FOption.empty());
			}
		}
		s_logger.info("deleteds: {}", deletedIdxes.length);
		
		return deletedIdxes.length;
	}
	
	private int writeUpdateds(ShapefileCompare shpCmp) throws IOException {
		List<Tuple<Integer,Integer>> mappings = shpCmp.getUpdateMappings().toList();
		if ( mappings.size() > 0 ) {
			File updatedsFile = new File(m_outputPath, "updates");
			try ( PrintWriter pw = new PrintWriter(new FileWriter(updatedsFile)) ) {
				FStream.from(mappings)
						.forEach(t -> pw.printf("%d,%d%n", t._1, t._2));
			}
			
			if ( m_writeShp ) {
				List<SimpleFeature> features = shpCmp.getUpdatedFeatures().toList();
				File updatedsDir = new File(m_outputPath, "updateds_shp");
				Shapefile.writeShapefile(updatedsDir, features, Charset.defaultCharset(),
											FOption.empty(), FOption.empty());
			}
		}
		s_logger.info("deleteds: {}", mappings.size());
		
		return mappings.size();
	}
	
	private int writeInserteds(ShapefileCompare shpCmp) throws IOException {
		int[] insertedIdxes = shpCmp.getInsertedFeatureSeqNo().toArray();
		if ( insertedIdxes.length > 0 ) {
			File insertedsFile = new File(m_outputPath, "inserteds");
			try ( PrintWriter pw = new PrintWriter(new FileWriter(insertedsFile)) ) {
				IntFStream.of(insertedIdxes).forEach(pw::println);
			}
			
			if ( m_writeShp ) {
				List<SimpleFeature> features = shpCmp.getInsertedFeatures().toList();
				File insertedsDir = new File(m_outputPath, "inserteds_shp");
				Shapefile.writeShapefile(insertedsDir, features, Charset.defaultCharset(),
										FOption.empty(), FOption.empty());
			}
		}
		s_logger.info("inserteds: {}", insertedIdxes.length);
		
		return insertedIdxes.length;
	}
	
	public static File getLog4jPropertiesFile() {
		String homeDir = FOption.ofNullable(System.getenv("SHPDIFF_HOME"))
								.getOrElse(() -> System.getProperty("user.dir"));
		return new File(homeDir, "log4j.properties");
	}
	
	public static File configureLog4j() throws IOException {
		File propsFile = getLog4jPropertiesFile();
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		PropertyConfigurator.configure(props);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
		
		return propsFile;
	}
}
