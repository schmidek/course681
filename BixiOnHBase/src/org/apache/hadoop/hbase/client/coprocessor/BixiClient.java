package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BixiProtocol;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class BixiClient {
  public static final Log log = LogFactory.getLog(BixiClient.class);

  HTable table, cluster_table;
  Configuration conf;
  private static final byte[] TABLE_NAME = Bytes.toBytes("Station_Statistics");
  private static final byte[] CLUSTER_TABLE_NAME = Bytes.toBytes("Station_Cluster");

  public BixiClient(Configuration conf) throws IOException {
    this.conf = conf;
    this.table = new HTable(conf, TABLE_NAME);
    this.cluster_table = new HTable(conf, CLUSTER_TABLE_NAME);
    log.debug("in constructor of BixiClient");
  }

  /**
* @param stationIds
* @param dateWithHour
* : most simple format; format is: dd_mm_yyyy__hh
* @return //01_10_2010__01
* @throws Throwable
* @throws IOException
*/
  public <R> Map<String, Integer> getAvailBikes(final List<String> stationIds,
      String dateWithHour) throws IOException, Throwable {
    final Scan scan = new Scan();
    log.debug("in getAvailBikes: " + dateWithHour);
    if (dateWithHour != null) {
      scan.setStartRow((dateWithHour + "_00").getBytes());
      scan.setStopRow((dateWithHour + "_59").getBytes());
    }
    class BixiCallBack implements Batch.Callback<Map<String, Integer>> {
      Map<String, Integer> res = new HashMap<String, Integer>();

      @Override
      public void update(byte[] region, byte[] row, Map<String, Integer> result) {
        log.debug("in update, result is: " + result.toString());
        System.out.println("in update as a sop" + result.toString());
        res = result;
      }
    }
    BixiCallBack callBack = new BixiCallBack();
    table.coprocessorExec(BixiProtocol.class, scan.getStartRow(), scan
        .getStopRow(), new Batch.Call<BixiProtocol, Map<String, Integer>>() {
      public Map<String, Integer> call(BixiProtocol instance)
          throws IOException {
        return instance.giveAvailableBikes(0, stationIds, scan);
      };
    }, callBack);

    return callBack.res;
  }

  public Map<String, Integer> getAvgUsageForAHr(final List<String> stationIds,
      String dateWithHour) throws IOException, Throwable {
    final Scan scan = new Scan();
    log.debug("in getAvgUsageForAHr: " + dateWithHour);
    //PrefixFilter filter = new PrefixFilter(dateWithHour.getBytes());
    
    //scan.setFilter(filter);
    if (dateWithHour != null) {
      scan.setStartRow((dateWithHour).getBytes());
      scan.setStopRow((dateWithHour + "_ZZ").getBytes());
      if(stationIds!=null && stationIds.size()>0){
    	  String regex = "";
    	  boolean start = true;
    	  for(String sId : stationIds){
    		  if(!start)
    			  regex += "|";
    		  start = false;
    		  regex += sId;
    	  }
    	  Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    	  scan.setFilter(filter);
      }
    }
    class BixiCallBack implements Batch.Callback<Map<String, Integer>> {
      Map<String, Integer> res = new HashMap<String, Integer>();
      int count = 0;

      @Override
      public void update(byte[] region, byte[] row, Map<String, Integer> result) {
        count++;
        log.debug("in update, result is: " + result.toString());
        System.out.println("in update as a sop" + result.toString());
        for (Map.Entry<String, Integer> e : result.entrySet()) {
          if (res.containsKey(e.getKey())) { // add the val
            int t = e.getValue();
            t += res.get(e.getKey());
            res.put(e.getKey(), t);
          } else {
            res.put(e.getKey(), e.getValue());
          }
        }
      }

      private Map<String, Integer> getResult() {
        for (Map.Entry<String, Integer> e : res.entrySet()) {
          int i = e.getValue() / count;
          res.put(e.getKey(), i);
        }
        return res;
      }
    }

    BixiCallBack callBack = new BixiCallBack();
    table.coprocessorExec(BixiProtocol.class, scan.getStartRow(), scan
        .getStopRow(), new Batch.Call<BixiProtocol, Map<String, Integer>>() {
      public Map<String, Integer> call(BixiProtocol instance)
          throws IOException {
        return instance.giveAverageUsage(stationIds, scan);
      };
    }, callBack);

    return callBack.getResult();

  }

  // get number of free bikes at a given time. for a given pair of lat/lon and a
  // radius

  /**
* @param lat
* @param lon
* @param radius
* @param dateWithHour
* @return
* @throws IOException
* @throws Throwable
*/
  public Map<String, Double> getAvailableBikesFromAPoint(final double lat,
      final double lon, final double radius, String dateWithHour)
      throws IOException, Throwable {
	  
	  List<String> stationIds = this.getStationsNearPoint(lat, lon);
	  
	  final Scan scan = new Scan();
	  if (dateWithHour != null) {
	      scan.setStartRow((dateWithHour).getBytes());
	      scan.setStopRow((dateWithHour + "_ZZ").getBytes());
	      if(stationIds!=null && stationIds.size()>0){
	    	  String regex = "";
	    	  boolean start = true;
	    	  for(String sId : stationIds){
	    		  if(!start)
	    			  regex += "|";
	    		  start = false;
	    		  regex += sId;
	    	  }
	    	  Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
	    	  scan.setFilter(filter);
	      }
	    }
    class BixiAvailCallBack implements Batch.Callback<Map<String, Double>> {
      Map<String, Double> res = new HashMap<String, Double>();

      @Override
      public void update(byte[] region, byte[] row, Map<String, Double> result) {
        res.putAll(result);
      }

      private Map<String, Double> getResult() {
        return res;
      }
    }

    BixiAvailCallBack callBack = new BixiAvailCallBack();
    table.coprocessorExec(BixiProtocol.class, scan.getStartRow(), scan.getStopRow(),
        new Batch.Call<BixiProtocol, Map<String, Double>>() {
          public Map<String, Double> call(BixiProtocol instance)
              throws IOException {
            return instance.getAvailableBikesFromAPoint(lat, lon, radius, scan);
          };
        }, callBack);

    return callBack.getResult();

  }
  
  public List<String> getStationsNearPoint(final double lat, final double lon) throws IOException, Throwable{
	  
	  class BixiAvailCallBack implements Batch.Callback<List<String>> {
	      List<String> res = new ArrayList<String>();

	      @Override
	      public void update(byte[] region, byte[] row, List<String> result) {
	        res.addAll(result);
	      }

	      private List<String> getResult() {
	        return res;
	      }
	    }

	    BixiAvailCallBack callBack = new BixiAvailCallBack();
	    cluster_table.coprocessorExec(BixiProtocol.class, null, null,
	        new Batch.Call<BixiProtocol, List<String>>() {
	          public List<String> call(BixiProtocol instance)
	              throws IOException {
	            return instance.getStationsNearPoint(lat, lon);
	          };
	        }, callBack);

	    return callBack.getResult();
  }
}

