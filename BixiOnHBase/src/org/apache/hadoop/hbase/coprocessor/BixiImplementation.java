package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
* @author hv
*/
public class BixiImplementation extends BaseEndpointCoprocessor implements
    BixiProtocol {

  static final Log log = LogFactory.getLog(BixiImplementation.class);

  private static byte[] colFamilyStat = "statistics".getBytes();

  @Override
  public Map<String, Integer> getAverageUsage_Schema2(List<String> stationIds,
      Scan scan) throws IOException {

	System.err.println("scanning");
	scan.addFamily(colFamilyStat);

    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    List<KeyValue> res = new ArrayList<KeyValue>();
    Map<String, Integer> result = new HashMap<String, Integer>();
    boolean hasMoreResult = false;
    int rowCounter = 0;
    try {
      do {
        rowCounter++;
        hasMoreResult = scanner.next(res);
        for (KeyValue kv : res) {
        	System.err.println("got a kv: " + kv);
          String stationId = (kv.getKeyString().split(":")[1]).split("\\\\")[0];
          System.err.println("stationid: " + stationId);
          String value = new String(kv.getValue());
          System.err.println("value: " + value);
          Integer usage = Integer.parseInt(value.split(",")[1]);
          System.err.println("usage: " + usage);

          if(result.containsKey(stationId)){
        	  result.put(stationId, usage + result.get(stationId));
          }else{
        	  result.put(stationId, usage);
          }
        }
        res.clear();
      } while (hasMoreResult);
    } finally {
      scanner.close();
    }
    for (Map.Entry<String, Integer> e : result.entrySet()) {
    	System.err.println("counter and value is" + rowCounter + "," + e.getValue());
      int i = e.getValue() / rowCounter;
      result.put(e.getKey(), i);
    }
    return result;
  }

  /**
* make a general method that takes a pair of lat/lon and a radius and give a
* boolean whether it was in or out.
* @throws IOException
*/
  @Override
  public Map<String, Double> getAvailableBikesFromAPoint_Schema2(double lat,
      double lon, Scan scan) throws IOException {
	  scan.addFamily(colFamilyStat);
	  InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
		        .getRegion().getScanner(scan);
    Map<String, Double> result = new HashMap<String, Double>();
    boolean hasMoreResult = false;
    int rowCounter = 0;
    List<KeyValue> res = new ArrayList<KeyValue>();
    try {
      do {
        rowCounter++;
        hasMoreResult = scanner.next(res);
        for (KeyValue kv : res) {
        	System.err.println("got a kv: " + kv);
          String stationId = (kv.getKeyString().split(":")[1]).split("\\\\")[0];
          System.err.println("stationid: " + stationId);
          String value = new String(kv.getValue());
          System.err.println("value: " + value);
          Double free = Double.parseDouble(value.split(",")[0]);
          System.err.println("free: " + free);

          if(result.containsKey(stationId)){
        	  result.put(stationId, free + result.get(stationId));
          }else{
        	  result.put(stationId, free);
          }
        }
        res.clear();
      } while (hasMoreResult);
    } finally {
      scanner.close();
    }
    return result;
  }
  
  public List<String> getStationsNearPoint_Schema2(double lat, double lon) throws IOException {
      Scan scan = new Scan();
      InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
      .getRegion().getScanner(scan);
      boolean hasMoreResult = false;
      int rowCounter = 0;
      List<KeyValue> res = new ArrayList<KeyValue>();
      try {
        do {
          rowCounter++;
          hasMoreResult = scanner.next(res);
          for (KeyValue kv : res) {
        	if(!Bytes.toString(kv.getQualifier()).equalsIgnoreCase("ids")){
        		//only look at stationid column
        		continue;
        	}
          	System.err.println("got a kv: " + kv);
            String clusterId = Bytes.toString(kv.getRow());
            System.err.println("clusterId: " + clusterId);
            String[] parts = clusterId.split("-");
            double cLat = Double.parseDouble(parts[0]);
            double cLon = Double.parseDouble(parts[1]);
            double dx = Double.parseDouble(parts[2]);
            double dy = Double.parseDouble(parts[3]);
            double distx = lat-cLat;
            double disty = lon-cLon;
            if(distx >= 0 && distx <= dx && disty >= 0 && disty <= dy){
            	//get stations in cluster
            	return Arrays.asList(Bytes.toString(kv.getValue()).split(","));
            }
          }
          res.clear();
        } while (hasMoreResult);
      } finally {
        scanner.close();
      }
	  return new ArrayList<String>();
  }

}