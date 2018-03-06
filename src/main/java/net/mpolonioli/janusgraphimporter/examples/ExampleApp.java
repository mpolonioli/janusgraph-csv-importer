package net.mpolonioli.janusgraphimporter.examples;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.janusgraph.core.Cardinality;

import net.mpolonioli.janusgraphimporter.core.JanusGraphImporter;

public class ExampleApp {

	@SuppressWarnings({ "rawtypes", "serial" })
	public static void main(String[] args) {
		
		String s = File.separator;
		
		String conf = "example-resources" + s + "janusgraph-properties" + s + "janusgraph-hbase.properties";
		
		File vertexFile = new File("example-resources" + s + "data-csv" + s + "vertices" + s + "person.csv");
		File edgeFile = new File("example-resources" + s + "data-csv" + s + "edges" + s + "knowsFile.csv");
		
		List<String> vertexLabels = new ArrayList<>(Arrays.asList("person"));
		List<String> edgeLabels = new ArrayList<>(Arrays.asList("knows"));
		List<String> propertyKeys = new ArrayList<>(Arrays.asList("id", "name", "surname", "birthdate", "email"));
		List<String> propertiesWithIndex = new ArrayList<>(Arrays.asList("id"));
		HashMap<String, Cardinality> propertyHasCardinality = new HashMap<String, Cardinality>() {
			{
				put("birthdate", Cardinality.SINGLE); 
				put("surname", Cardinality.SINGLE);
				put("name", Cardinality.SINGLE);
				put("id", Cardinality.SINGLE);
				put("email", Cardinality.LIST);
			};
		};
		HashMap<String, Class> propertyHasType = new HashMap<String, Class>() {
			{
				put("birthdate", Date.class); 
				put("surname", String.class);
				put("name", String.class);
				put("id", Long.class);
				put("email", String.class);
			};
		};
		HashMap<String, String> edgeHasLabel = new HashMap<String, String>() {
			{
				put("knowsFile", "knows");
			};
		};
		
		
		boolean printLoadingDots = true;
		int batchSize = 20000;
		long progReportPeriod = 10;
		int threadCount = 4;
		boolean undirected = true;
		
		JanusGraphImporter importer = new JanusGraphImporter(conf);
		try {
			importer.clearGraph();
			importer.defineSchema(vertexLabels, edgeLabels, propertyKeys, propertyHasCardinality, propertyHasType, propertiesWithIndex);
			importer.loadVertices(vertexFile, printLoadingDots, batchSize, progReportPeriod, threadCount, propertyHasType, propertyHasCardinality);
			importer.loadEdges(edgeFile, edgeHasLabel, undirected, printLoadingDots, batchSize, progReportPeriod, threadCount, propertyHasType);
			importer.closeConnection();
		}catch(Exception e)
		{
			e.printStackTrace();
			importer.closeConnection();
		}
	}

}
