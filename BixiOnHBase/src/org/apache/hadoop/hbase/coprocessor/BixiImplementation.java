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

  private static byte[] colFamily = "statistics".getBytes();
  private final static String BIXI_DELIMITER = "#";
  private final static int BIXI_DATA_LENGTH = 11;
  private static byte[] stationTableName = "Station_Cluster".getBytes();
  private static byte[] stationIdsFamily = "stations".getBytes();

  @Override
  public Map<String, Integer> giveAvailableBikes(long milliseconds,
      List<String> stationIds, Scan scan) throws IOException {
    // scan has set the time stamp accordingly, i.e., the start and end row of
    // the scan.

    for (String qualifier : stationIds) {
      log.debug("adding qualifier: " + qualifier);
      scan.addColumn(colFamily, qualifier.getBytes());
    }
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    List<KeyValue> res = new ArrayList<KeyValue>();
    Map<String, Integer> result = new HashMap<String, Integer>();
    boolean hasMoreResult = false;
    try {
      do {
        hasMoreResult = scanner.next(res);
        for (KeyValue kv : res) {
          log.debug("got a kv: " + kv);
          int availBikes = getFreeBikes(kv);
          String id = Bytes.toString(kv.getQualifier());
          log.debug("result to be added is: " + availBikes + " id: " + id);
          result.put(id, availBikes);
        }
        res.clear();
      } while (hasMoreResult);
    } finally {
      scanner.close();
    }
    return result;
  }

  private int getFreeBikes(KeyValue kv) {
    String availBikes = processKV(kv, 9);
    log.debug("availbikes::" + availBikes);
    try {
      return Integer
          .parseInt(availBikes.substring(availBikes.indexOf("=") + 1));
    } catch (Exception e) {
      System.err.println("Non numeric value as avail bikes!");
    }
    return 0;
  }

  private String processKV(KeyValue kv, int index) {
    if (kv == null || index > 10 || index < 0)
      return null;
    log.debug("kv.getValue()" + Bytes.toString(kv.getValue()));
    String[] str = Bytes.toString(kv.getValue()).split(
        BixiImplementation.BIXI_DELIMITER);
    // malformed value (shouldn't had been here.
    if (str.length != BixiImplementation.BIXI_DATA_LENGTH)
      return null;
    return str[index];
  }

  @Override
  public Map<String, Integer> giveAverageUsage(List<String> stationIds,
      Scan scan) throws IOException {

	System.err.println("scanning");
	scan.addFamily(colFamily);

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

  private int getEmptyDocks(KeyValue kv) {
    String availBikes = processKV(kv, 10);
    log.debug("emptyDocks::" + availBikes);
    try {
      return Integer
          .parseInt(availBikes.substring(availBikes.indexOf("=") + 1));
    } catch (Exception e) {
      System.err.println("Non numeric value as avail bikes!");
    }
    return 0;
  }

  /**
* make a general method that takes a pair of lat/lon and a radius and give a
* boolean whether it was in or out.
* @throws IOException
*/
  @Override
  public Map<String, Double> getAvailableBikesFromAPoint(double lat,
      double lon, double radius, Scan scan) throws IOException {
	  scan.addFamily(colFamily);
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
  
  public List<String> getStationsNearPoint(double lat, double lon) throws IOException {
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

  final static double RADIUS = 6371;

}