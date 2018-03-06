package net.mpolonioli.janusgraphimporter.core;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;

import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;

public class LoadVerticiesThread extends Thread {
	
	private JanusGraph graph;
	private String[] colNames;
	private String vertexLabel;
	private long txMaxRetries;
	private String[] threadLines;
	private long lineCount;
	private @SuppressWarnings("rawtypes") HashMap<String, Class> propertyHasType;
	private HashMap<String, Cardinality> propertyHasCardinality;
	
	public LoadVerticiesThread(
			JanusGraph graph,
			String[] colNames,
			String vertexLabel,
			long txMaxRetries,
			String[] threadLines,
			long lineCount,
			@SuppressWarnings("rawtypes") HashMap<String, Class> propertyHasType,
			HashMap<String, Cardinality> propertyHasCardinality
			) {
		this.graph = graph;
		this.colNames = colNames;
		this.vertexLabel = vertexLabel;
		this.threadLines = threadLines;
		this.lineCount = lineCount;
		this.propertyHasType = propertyHasType;
		this.propertyHasCardinality = propertyHasCardinality;
	}

	@Override
	public void run() {
		

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		boolean txSucceeded = false;
		int txFailCount = 0;
		do {
			JanusGraphTransaction tx = graph.newTransaction();
						
			for (int i = 0; i < threadLines.length; i++) {

				String line = threadLines[i];

				String[] colVals = line.split("\\|");

				List<Object> keyValues = new ArrayList<Object>();
				HashMap<String, List<Object>> propertyHasValues = new HashMap<>();
				for (int j = 0; j < colVals.length; ++j) {
					String propertyName = colNames[j];
					String valueLine = colVals[j];
					if(!valueLine.equals(""))
					{
						if(propertyHasCardinality.get(propertyName).equals(Cardinality.SINGLE))
						{
							switch(propertyHasType.get(propertyName).getName())
							{
							case "java.lang.String" :
							{
								keyValues.add(propertyName);
								keyValues.add(valueLine);
								break;
							}
							case "java.lang.Boolean" :
							{
								keyValues.add(propertyName);
								keyValues.add(Boolean.parseBoolean(valueLine));
								break;
							}
							case "java.lang.Long" :
							{
								keyValues.add(propertyName);
								keyValues.add(Long.parseLong(valueLine));
								break;
							}
							case "java.lang.Integer" :
							{
								keyValues.add(propertyName);
								keyValues.add(Integer.parseInt(valueLine));
								break;
							}
							case "java.util.Date" :
							{
								try {
									Date date = dateFormat.parse(valueLine);
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

						}else
						{
							propertyHasValues.put(propertyName, new ArrayList<Object>());
							String[] stringValues = colVals[j].split(";");
							for(int v = 0; v < stringValues.length; v++)
							{
								String stringValue = stringValues[v];

								switch(propertyHasType.get(propertyName).getName())
								{
								case "java.lang.String" :
								{
									propertyHasValues.get(propertyName).add(stringValue);
									break;
								}
								case "java.lang.Boolean" :
								{
									propertyHasValues.get(propertyName).add(Boolean.parseBoolean(stringValue));
									break;
								}
								case "java.lang.Long" :
								{
									propertyHasValues.get(propertyName).add(Long.parseLong(stringValue));
									break;
								}
								case "java.lang.Integer" :
								{
									propertyHasValues.get(propertyName).add(Integer.parseInt(stringValue));
									break;
								}
								case "java.util.Date" :
								{
									try {
										Date date = dateFormat.parse(valueLine);
										propertyHasValues.get(propertyName).add(date);
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
					}
				}

				keyValues.add(T.label);
				keyValues.add(vertexLabel);
				
				// loading the vertex with the property with cardinality Single
				JanusGraphVertex addedVertex = tx.addVertex(keyValues.toArray());
				
				// loading all properties with cardinality List to the vertex
				for(String key : propertyHasValues.keySet())
				{
					for(Object value : propertyHasValues.get(key))
					{
						addedVertex.property(propertyHasCardinality.get(key).convert(), key, value);
					}
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
