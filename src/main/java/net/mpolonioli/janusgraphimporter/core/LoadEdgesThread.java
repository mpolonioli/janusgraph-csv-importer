package net.mpolonioli.janusgraphimporter.core;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TimeZone;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

public class LoadEdgesThread extends Thread {
		
	private JanusGraph graph;
	private long txMaxRetries;
	private String[] threadLines;
	private long lineCount;
	private String edgeLabel;
	private boolean undirected;
	private String[] colNames;
	private @SuppressWarnings("rawtypes") HashMap<String, Class> propertyHasType;
	
	public LoadEdgesThread(
			JanusGraph graph,
			String edgeLabel,
			boolean undirected,
			long txMaxRetries,
			String[] threadLines,
			long lineCount,
			String[] colNames,
			@SuppressWarnings("rawtypes") HashMap<String, Class> propertyHasType)
	{
		this.graph = graph;
		this.txMaxRetries = txMaxRetries;
		this.threadLines = threadLines;
		this.lineCount = lineCount;
		this.edgeLabel = edgeLabel;
		this.undirected = undirected;
		this.propertyHasType = propertyHasType;
		this.colNames = colNames;
	}
	
	@Override
	public void run() {
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		boolean txSucceeded = false;
		int txFailCount = 0;
		
		String keyLabelV1 = colNames[0];
		String keyLabelV2 = colNames[1];
		
		do {
			JanusGraphTransaction tx = graph.newTransaction();
			for (int i = 0; i < threadLines.length; i++) {
				String line = threadLines[i];

				String[] colVals = line.split("\\|");
				String keyValueV1 = colVals[0];
				String keyValueV2 = colVals[1];
				
				GraphTraversalSource g = tx.traversal();
				
				try
				{
					// find the vertices
					Vertex vertex1 = 
							g.V().has(keyLabelV1, keyValueV1).next();
					Vertex vertex2 = 
							g.V().has(keyLabelV2, keyValueV2).next();
					
					// add the properties to the edge if exists
					List<Object> keyValues = new ArrayList<Object>();
					for (int j = 2; j < colVals.length; ++j) {
						
						String propertyName = colNames[j];
						String stringValue = colVals[j];
						
						if(!stringValue.equals(""))
						{
							switch(propertyHasType.get(propertyName).getName())
							{
							case "java.lang.String" :
							{
								keyValues.add(propertyName);
								keyValues.add(stringValue);
								break;
							}
							case "java.lang.Boolean" :
							{
								keyValues.add(propertyName);
								keyValues.add(Boolean.parseBoolean(stringValue));
								break;
							}
							case "java.lang.Long" :
							{
								keyValues.add(propertyName);
								keyValues.add(Long.parseLong(stringValue));
								break;
							}
							case "java.lang.Integer" :
							{
								keyValues.add(propertyName);
								keyValues.add(Integer.parseInt(stringValue));
								break;
							}
							case "java.util.Date" :
							{
								try {
									Date date = dateFormat.parse(stringValue);
									keyValues.add(propertyName);
									keyValues.add(date);
								} catch (ParseException e) {
									e.printStackTrace();
								}
							}
							default :
							{
								break;
							}
							}
						}
					}
					
					vertex1.addEdge(edgeLabel, vertex2, keyValues.toArray());
					
					if (undirected) {
						vertex2.addEdge(edgeLabel, vertex1, keyValues.toArray());
					}

				}catch(NoSuchElementException e)
				{
					System.out.println("vertex not found: "+ keyLabelV1 + "=" + keyValueV1 + " -" + edgeLabel + "-> " + keyLabelV2 + "=" + keyValueV2);
				}
			}

			try {
				tx.commit();
				txSucceeded = true;
			} catch (Exception e) {
				txFailCount++;
			}

			if (txFailCount > txMaxRetries) {
				throw new RuntimeException(String.format(
						"ERROR: Transaction failed %d times, (file lines [%d,%d])" +  
								"aborting...", txFailCount, lineCount + 2, (lineCount + 2) + (threadLines.length - 1)));
			}
		} while (!txSucceeded);
	}
}
