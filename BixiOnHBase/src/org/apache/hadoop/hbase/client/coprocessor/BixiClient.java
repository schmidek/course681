package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
import org.apache.hadoop.hbase.coprocessor.TotalNum;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import bixi.hbase.query.BixiConstant;

public class BixiClient {
  public static final Log log = LogFactory.getLog(BixiClient.class);

  HTable table, stat_table, cluster_table;;
  Configuration conf;
  private static final byte[] TABLE_NAME = Bytes.toBytes(BixiConstant.SCHEMA1_TABLE_NAME);
  private static final byte[] STATION_TABLE_NAME = Bytes.toBytes(BixiConstant.SCHEMA2_BIKE_TABLE_NAME);
  private static final byte[] STATION_CLUSTER_TABLE_NAME = Bytes.toBytes(BixiConstant.SCHEMA2_CLUSTER_TABLE_NAME);

  public BixiClient(Configuration conf) throws IOException {
    this.conf = conf;
    this.table = new HTable(conf, TABLE_NAME);
    this.stat_table = new HTable(conf, STATION_TABLE_NAME);
    this.cluster_table = new HTable(conf, STATION_CLUSTER_TABLE_NAME);
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
      scan.setStopRow((dateWithHour + "_60").getBytes());
    }
    class BixiCallBack implements Batch.Callback<Map<String, Integer>> {
      Map<String, Integer> res = new HashMap<String, Integer>();

      @Override
      public void update(byte[] region, byte[] row, Map<String, Integer> result) {
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

  public Map<String, Double> getAvgUsageForPeriod(final List<String> stationIds,
      String startDate, String endDate) throws IOException, Throwable {
    final Scan scan = new Scan();
    if(endDate == null)
    	endDate = startDate;
    if (startDate != null) {
      String startRow;
      String endRow;
      if(startDate.compareTo(endDate)<0){
    	  startRow = startDate;
    	  endRow = endDate;
      }else{
    	  startRow = endDate;
    	  endRow = startDate;
      }
      
      scan.setStartRow((startRow + "_00").getBytes());
      scan.setStopRow((endRow + "_60").getBytes());
      
      DateFormat format = new SimpleDateFormat("dd_MM_yyyy__HH");
      Date startD = format.parse(startDate);
      Date endD = format.parse(endDate);
      Calendar c = Calendar.getInstance();
      c.setTime(startD);

      DateFormat filterFormat = new SimpleDateFormat("dd_MM_yyyy__");
      
      String regex = "^(";
	  boolean start = true;
	  while(c.getTime().before(endD)){
		  if(!start)
			  regex += "|";
	 	  start = false;
		  regex += filterFormat.format(c.getTime());
		  c.add(Calendar.DATE, 1);
	  }
	  regex += ")";
	  Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
	  scan.setFilter(filter);
    }
    final long starttime = System.currentTimeMillis();
    class BixiCallBack implements Batch.Callback<Map<String, TotalNum>> {
      Map<String, TotalNum> res = new HashMap<String, TotalNum>();

      @Override
      public void update(byte[] region, byte[] row, Map<String, TotalNum> result) {
    	  long node_access = System.currentTimeMillis();
  		  System.out.println("node return time for " + Bytes.toString(region) + ": "
  				+ (node_access - starttime));
        for (Map.Entry<String, TotalNum> e : result.entrySet()) {
          if (res.containsKey(e.getKey())) { // add the val
            TotalNum tnnew = e.getValue();
            TotalNum restn = res.get(e.getKey());
            restn.merge(tnnew);
            res.put(e.getKey(), restn);
          } else {
            res.put(e.getKey(), e.getValue());
          }
        }
      }

      private Map<String, Double> getResult() {
    	Map<String, Double> ret = new HashMap<String, Double>();
        for (Map.Entry<String, TotalNum> e : res.entrySet()) {
          TotalNum tn = e.getValue();
          double i = tn.total / (double)tn.num;
          ret.put(e.getKey(), i);
        }
        return ret;
      }
    }

    BixiCallBack callBack = new BixiCallBack();
    table.coprocessorExec(BixiProtocol.class, scan.getStartRow(), scan
        .getStopRow(), new Batch.Call<BixiProtocol, Map<String, TotalNum>>() {
      public Map<String, TotalNum> call(BixiProtocol instance)
          throws IOException {
        return instance.giveTotalUsage(stationIds, scan);
      };
    }, callBack);
    long cluster_access = System.currentTimeMillis();
	System.out.println("total execution time : "
			+ (cluster_access - starttime));
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
  public Map<String, Integer> getAvailableBikesFromAPoint(final double lat,
      final double lon, final double radius, String dateWithHour)
      throws IOException, Throwable {
    final Get get = new Get((dateWithHour + "_00").getBytes());
    log.debug("in getAvgUsageForAHr: " + dateWithHour);
    class BixiAvailCallBack implements Batch.Callback<Map<String, Integer>> {
      Map<String, Integer> res = new HashMap<String, Integer>();

      @Override
      public void update(byte[] region, byte[] row, Map<String, Integer> result) {
        res = result;
      }

      private Map<String, Integer> getResult() {
        return res;
      }
    }

    BixiAvailCallBack callBack = new BixiAvailCallBack();
    long starttime = System.currentTimeMillis();
    table.coprocessorExec(BixiProtocol.class, get.getRow(), get.getRow(),
        new Batch.Call<BixiProtocol, Map<String, Integer>>() {
          public Map<String, Integer> call(BixiProtocol instance)
              throws IOException {
            return instance.getAvailableBikesFromAPoint(lat, lon, radius, get);
          };
        }, callBack);
    long cluster_access = System.currentTimeMillis();
	System.out.println("total execution time : "
			+ (cluster_access - starttime));
	Map<String, Integer> res = callBack.getResult();
	System.out.println("Number of stations: " + res.size());
    return res;

  }
  
  /* Schema 2 implementation */
  
  public Map<String, Double> getAvgUsageForPeriod_Schema2(final List<String> stationIds,
	      String startDateWithHour, String endDateWithHour) throws IOException, Throwable {
	    final Scan scan = new Scan();
	    log.debug("in getAvgUsageForPeriod: " + startDateWithHour);
	    if(endDateWithHour == null){
	    	endDateWithHour = startDateWithHour;
	    }
	    if (startDateWithHour != null) {
	      scan.setStartRow((startDateWithHour + "-01").getBytes());
	      scan.setStopRow((endDateWithHour + "-408").getBytes());
	      if(stationIds!=null && stationIds.size()>0){
	    	  String regex = "(";
	    	  boolean start = true;
	    	  for(String sId : stationIds){
	    		  //String id = Integer.toString(Integer.parseInt(sId));
	    		  if(!start)
	    			  regex += "|";
	    		  start = false;
	    		  regex += "-" + sId;
	    	  }
	    	  regex += ")$";
		  System.out.println("REGEX: " + regex);
	    	  Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
	    	  scan.setFilter(filter);
	      }
	    }
	    final long starttime = System.currentTimeMillis();
	    class BixiCallBack implements Batch.Callback<Map<String, TotalNum>> {
	      Map<String, TotalNum> res = new HashMap<String, TotalNum>();

	      @Override
	      public void update(byte[] region, byte[] row, Map<String, TotalNum> result) {
	    	  long node_access = System.currentTimeMillis();
	  		  System.out.println("node return time for " + Bytes.toString(region) + ": "
	  				+ (node_access - starttime));
	    	  for (Map.Entry<String, TotalNum> e : result.entrySet()) {
	    		  if (res.containsKey(e.getKey())) { // add the val
	    			  TotalNum tnnew = e.getValue();
	    			  TotalNum restn = res.get(e.getKey());
	    			  restn.merge(tnnew);
	    			  res.put(e.getKey(), restn);
	    		  } else {
	    			  res.put(e.getKey(), e.getValue());
	    		  }
	    	  }
	      }

	      private Map<String, Double> getResult() {
	    	  Map<String, Double> ret = new HashMap<String, Double>();
	          for (Map.Entry<String, TotalNum> e : res.entrySet()) {
	            TotalNum tn = e.getValue();
	            double i = tn.total / (double)tn.num;
	            ret.put(e.getKey(), i);
	          }
	          return ret;
	      }
	    }

	    BixiCallBack callBack = new BixiCallBack();
	    stat_table.coprocessorExec(BixiProtocol.class, scan.getStartRow(), scan
	        .getStopRow(), new Batch.Call<BixiProtocol, Map<String, TotalNum>>() {
	      public Map<String, TotalNum> call(BixiProtocol instance)
	          throws IOException {
	        return instance.getTotalUsage_Schema2(scan);
	      };
	    }, callBack);
	    long cluster_access = System.currentTimeMillis();
		System.out.println("execution time : "
				+ (cluster_access - starttime));
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
	  public Map<String, Integer> getAvailableBikesFromAPoint_Schema2(final double lat,
	      final double lon, String dateWithHour)
	      throws IOException, Throwable {
		  long totalstarttime = System.currentTimeMillis();
		  List<String> stationIds = this.getStationsNearPoint(lat, lon);
		  if(stationIds==null || stationIds.size()<=0){
			  System.out.println("NO STATIONS FOUND.");
			  return new HashMap<String, Integer>();
		  }
		  
		  final Scan scan = new Scan();
		  if (dateWithHour != null) {
			  scan.setStartRow((dateWithHour + "-01").getBytes());
			  scan.setStopRow((dateWithHour + "-408").getBytes());
			  String regex = "(";
			  boolean start = true;
			  for(String sId : stationIds){
				  if(!start)
					  regex += "|";
				  start = false;
				  regex += "-" + sId;
			  }
			  regex += ")$";
			  Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
			  scan.setFilter(filter);
		  }
	    class BixiAvailCallBack implements Batch.Callback<Map<String, Integer>> {
	      Map<String, Integer> res = new HashMap<String, Integer>();

	      @Override
	      public void update(byte[] region, byte[] row, Map<String, Integer> result) {
	        res.putAll(result);
	      }

	      private Map<String, Integer> getResult() {
	        return res;
	      }
	    }

	    BixiAvailCallBack callBack = new BixiAvailCallBack();
	    long starttime = System.currentTimeMillis();
	    stat_table.coprocessorExec(BixiProtocol.class, scan.getStartRow(), scan.getStopRow(),
	        new Batch.Call<BixiProtocol, Map<String, Integer>>() {
	          public Map<String, Integer> call(BixiProtocol instance)
	              throws IOException {
	            return instance.getAvailableBikesFromAPoint_Schema2(scan);
	          };
	        }, callBack);
	    long cluster_access = System.currentTimeMillis();
		System.out.println("statistics table access time : "
				+ (cluster_access - starttime));
		long totalaccess = System.currentTimeMillis();
		System.out.println("Total execution time : "
				+ (totalaccess - totalstarttime));
		Map<String, Integer> res = callBack.getResult();
	    return res;

	  }
	  
	  public List<String> getStationsNearPoint(final double lat, final double lon) throws IOException, Throwable{
		  System.out.println("Getting stations in cluster");
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
		    long starttime = System.currentTimeMillis();
		    cluster_table.coprocessorExec(BixiProtocol.class, null, null,
		        new Batch.Call<BixiProtocol, List<String>>() {
		          public List<String> call(BixiProtocol instance)
		              throws IOException {
		            return instance.getStationsNearPoint_Schema2(lat, lon);
		          };
		        }, callBack);
		    long cluster_access = System.currentTimeMillis();
			System.out.println("cluster table access time : "
					+ (cluster_access - starttime));
			List<String> res = callBack.getResult();
			System.out.println("got " + res.size() + " stations");
		    return res;
	  }
  
}

