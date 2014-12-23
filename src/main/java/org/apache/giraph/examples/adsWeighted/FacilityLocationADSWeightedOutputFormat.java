package org.apache.giraph.examples.adsWeighted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FacilityLocationADSWeightedOutputFormat extends TextVertexOutputFormat<LongWritable, FacilityLocationADSWeightedVertexValue, FloatWritable> {

	Random randomGenerator = new Random();
	
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new FacilityLocationADSTextVertexLineWriter();
	}

	  /**
	* Outputs for each line the vertex id and the ADS
	*/
	  private class FacilityLocationADSTextVertexLineWriter extends
	          TextVertexWriterToEachLine {

		@Override
		protected Text convertVertexToLine(Vertex<LongWritable, FacilityLocationADSWeightedVertexValue, FloatWritable, ?> vertex) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append(vertex.getId());
			sb.append("\t");
			
			// ArrayList<Double> vertexADS = vertex.getValue().getADS();
			// Map<Double, Double> vertexADS = vertex.getValue().getADS();
			
			float randomNum = randomGenerator.nextFloat()*1000; // some random number for Facility weights
			
			sb.append(randomNum);
			sb.append("\t");
			
			// String tmp1 = "";
			for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
				// tmp1 += edge.getTargetVertexId().get() + ":" + edge.getValue() + ";";
				sb.append(edge.getTargetVertexId().get());
				sb.append(":");
				sb.append(edge.getValue());
				sb.append(";");
			}
			
			sb.append("\t");
			
			Map <Double, Map<Double, Double>> vertexADSTmp = vertex.getValue().getADSTmp();

			for (Entry<Double, Map<Double, Double>> entry : vertexADSTmp.entrySet()) {
				
				Map<Double, Double> tmp = entry.getValue();
				for (Entry<Double, Double> entry1 : tmp.entrySet()) {
					sb.append(entry1.getKey());
					sb.append(":");
					sb.append(entry1.getValue());
					sb.append(";");
				}
			}
			
			return new Text(sb.toString());
		}
		  
	  }
	
}
