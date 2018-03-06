package net.mpolonioli.janusgraphimporter.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

public class JanusGraphImporter {
	
	private static final long TX_MAX_RETRIES = 1000;
	private static JanusGraph graph;
	
	public JanusGraphImporter(String conf)
	{
		graph = JanusGraphFactory.open(conf);
	}
	
	/*
	 * define the schema into JanusGraph
	 */
	public void defineSchema(
			List<String> vertexLabels,
			List<String> edgeLabels,
			List<String> propertyKeys,
			HashMap<String, Cardinality> propertyHasCardinality,
			@SuppressWarnings("rawtypes") HashMap<String, Class> propertyHasType,
			List<String> propertiesWithIndex
			)
	{
		
		openConnection();
		
		JanusGraphManagement mgmt;
		
		// Declare all vertex labels
		System.out.println("Declaring all vertex labels");
		for( String vLabel : vertexLabels ) {
			System.out.print(vLabel + " ");
			mgmt = graph.openManagement();
			mgmt.makeVertexLabel(vLabel).make();
			mgmt.commit();
		}
		
		// Declare all edge labels
		System.out.println("\nDeclaring all edge labels");
		for( String eLabel : edgeLabels ) {
			System.out.print(eLabel + " ");
			mgmt = graph.openManagement();
			mgmt.makeEdgeLabel(eLabel).multiplicity(Multiplicity.SIMPLE).make();
			mgmt.commit();
		}
		
		// Declare all properties and the relatives composite indexes
		System.out.println("\nDeclaring all properties with Cardinality.SINGLE");
		for ( String propKey : propertyKeys ) {
			System.out.print(propKey + " ");
			mgmt = graph.openManagement();
			PropertyKey property = mgmt.makePropertyKey(propKey).dataType(propertyHasType.get(propKey))
					.cardinality(propertyHasCardinality.get(propKey)).make();
			if(propertiesWithIndex.contains(propKey))
			{
				String indexLabel = propKey + "Index";
				System.out.print(indexLabel + " ");
				mgmt.buildIndex(indexLabel, Vertex.class).addKey(property).buildCompositeIndex();
			}
			mgmt.commit();
		}

		graph.tx().commit();
		System.out.println();
	}
	
	/*
	 * open a connection to the DBMS if close
	 */
	public void openConnection() {
		if(graph.isClosed())
		{
			graph = JanusGraphFactory.open(graph.configuration());
		}
	}
	
	/*
	 * close a connection to the DBMS if open
	 */
	public void closeConnection() {
		if(graph.isOpen())
		{
			graph.close();
		}
	}
	
	/*
	 * clear the existing graph
	 */
	public void clearGraph() {
		closeConnection();
		org.janusgraph.core.util.JanusGraphCleanup.clear(graph);
		openConnection();
	}
	
	/*
	 * load the vertices contained in the given file
	 */
	public void loadVertices(
			File file, 
			boolean printLoadingDots,
			int batchSize,
			long progReportPeriod,
			int threadCount,
			@SuppressWarnings("rawtypes") HashMap<String, Class> propertyHasType,
			HashMap<String, Cardinality> propertyHasCardinality
			) throws IOException, java.text.ParseException, InterruptedException {
		
		openConnection();

		String fileName = file.getName();
		String vertexLabel = fileName.substring(0, fileName.length() - 4);
		
		Scanner fileScanner = new Scanner(file);

		final String[] colNames = fileScanner.nextLine().split("\\|");
				
		long lineCount = 0;

		// For progress reporting
		long startTime = System.currentTimeMillis();
		long nextProgReportTime = startTime + progReportPeriod*1000;
		long lastLineCount = 0;
			
		while(fileScanner.hasNextLine())
		{
			int batchIndex = 0;
			List<String> batchLines = new ArrayList<>();
			while(batchIndex < batchSize && fileScanner.hasNextLine())
			{
				batchLines.add(fileScanner.nextLine());
				batchIndex++;
			}
			
			lineCount += batchLines.size();
			
			List<Thread> threads = new ArrayList<>();
			for(int t = 0; t < threadCount; t++)
			{
				int threadStartIndex = ((batchSize / threadCount) * t);
				if (threadStartIndex >= batchLines.size())
				{
					break;
				}
				final List<String> threadLines = batchLines.subList(
						threadStartIndex, 
						Math.min(threadStartIndex + (batchSize / threadCount), batchLines.size())
						);
				
				Thread thread = new LoadVerticiesThread(
						graph,
						colNames,
						vertexLabel,
						TX_MAX_RETRIES,
						threadLines.toArray(new String[0]),
						lineCount,
						propertyHasType,
						propertyHasCardinality
						);
				
				thread.setName("t" + t);
				threads.add(thread);
				thread.start();
			}
			for(Thread thread : threads)
			{
				thread.join();
			}


			if (printLoadingDots && 
					(System.currentTimeMillis() > nextProgReportTime)) {
				long timeElapsed = System.currentTimeMillis() - startTime;
				long linesLoaded = lineCount - lastLineCount;
				System.out.println(String.format(
						"Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
						(timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
				nextProgReportTime += progReportPeriod*1000;
				lastLineCount = lineCount;
			}
		}
		fileScanner.close();
		
		long timeElapsed = System.currentTimeMillis() - startTime;
		long linesLoaded = lineCount - lastLineCount;
		System.out.println(String.format(
				"Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
				(timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
		nextProgReportTime += progReportPeriod*1000;
		lastLineCount = lineCount;
	}
	
	/*
	 * load the edges contained in the given file
	 */
	public void loadEdges(
			File file,
			HashMap<String, String> edgeHasLabel,
			boolean undirected,
			boolean printLoadingDots,
			int batchSize,
			long progReportPeriod,
			int threadCount,
			@SuppressWarnings("rawtypes") HashMap<String, Class> propertyHasType
			) throws IOException,  java.text.ParseException, InterruptedException {

		openConnection();

		String fileName = file.getName();
		String edgeName = fileName.substring(0, fileName.length() - 4);
		
		String edgeLabel;
		if(edgeHasLabel.containsKey(edgeName))
		{
			edgeLabel = edgeHasLabel.get(edgeName);

		}else
		{
			edgeLabel = edgeName;
		}

		Scanner fileScanner = new Scanner(file);

		final String[] colNames = fileScanner.nextLine().split("\\|");
		
		long lineCount = 0;

		// For progress reporting
		long startTime = System.currentTimeMillis();
		long nextProgReportTime = startTime + progReportPeriod*1000;
		long lastLineCount = 0;

		while(fileScanner.hasNextLine())
		{
			int batchIndex = 0;
			List<String> batchLines = new ArrayList<>();
			while(batchIndex < batchSize && fileScanner.hasNextLine())
			{
				batchLines.add(fileScanner.nextLine());
				batchIndex++;
			}
			
			lineCount += batchLines.size();
			
			List<Thread> threads = new ArrayList<>();
			for(int t = 0; t < threadCount; t++)
			{
				int threadStartIndex = ((batchSize / threadCount) * t);
				if (threadStartIndex >= batchLines.size())
				{
					break;
				}
				final List<String> threadLines = batchLines.subList(
						threadStartIndex, 
						Math.min(threadStartIndex + (batchSize / threadCount), batchLines.size())
						);

				Thread thread = new LoadEdgesThread(
						graph, 
						edgeLabel, 
						undirected, 
						TX_MAX_RETRIES, 
						threadLines.toArray(new String[0]), 
						lastLineCount,
						colNames,
						propertyHasType
						);

				thread.setName("t" + t);
				threads.add(thread);
				thread.start();
			}
			for(Thread thread : threads)
			{
				thread.join();
			}

			if (printLoadingDots && 
					(System.currentTimeMillis() > nextProgReportTime)) {
				long timeElapsed = System.currentTimeMillis() - startTime;
				long linesLoaded = lineCount - lastLineCount;
				System.out.println(String.format(
						"Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
						(timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
				nextProgReportTime += progReportPeriod*1000;
				lastLineCount = lineCount;
			}
		}
		fileScanner.close();
		
		long timeElapsed = System.currentTimeMillis() - startTime;
		long linesLoaded = lineCount - lastLineCount;
		System.out.println(String.format(
				"Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
				(timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
		nextProgReportTime += progReportPeriod*1000;
		lastLineCount = lineCount;
	}

}
