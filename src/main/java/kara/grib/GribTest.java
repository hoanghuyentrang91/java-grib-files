package kara.grib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import ucar.ma2.Array;
import ucar.nc2.dataset.CoordinateAxis;
import ucar.nc2.dataset.CoordinateAxis1DTime;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.grid.GeoGrid;
import ucar.nc2.dt.grid.GridDataset;
import ucar.nc2.grib.grib2.Grib2Record;
import ucar.nc2.grib.grib2.Grib2RecordScanner;
import ucar.nc2.grib.grib2.Grib2SectionDataRepresentation;
import ucar.nc2.time.CalendarDate;
import ucar.unidata.io.RandomAccessFile;

public class GribTest 
{
	private final static String fullPath = "C:/Users/ryu/Desktop/data/wavecloud-store/201805/20180522/um/g512_v070_ergl_pres_h000.2018052212.gb2";
    public static void main( String[] args ) throws IOException, InterruptedException 
    {
    	long startTime = System.currentTimeMillis();
    	int stepsNum = 10;
    	int bandNum = 19;
    	String firstDataFile = fullPath.substring(fullPath.lastIndexOf("/") + 1);
		String currentStep = firstDataFile.substring(21,24);
		String timeStep;
		String var = "Temperature_isobaric";
		float lon = 126.0f;
		float lat = 38.0f;
		
		float[][] data = new float[bandNum][stepsNum];
		
		final List<Callable<Array>> callables = new ArrayList<>();
		final ExecutorService pool = Executors.newFixedThreadPool(stepsNum);
		
		for (int iTime = 0; iTime < stepsNum; iTime++) {
	    	timeStep = String.format("%03d", (Integer.parseInt(currentStep) + 3 * iTime));
			String file =  firstDataFile.replace(("h" + currentStep), ("h" + timeStep));
			String absPath = fullPath.replace(firstDataFile, file);
			
			callables.add(new NwpCallable(absPath, lon, lat, var));
		}
		
		List<Future<Array>> futures = pool.invokeAll(callables);
		try {
			  for (final Future<Array> future : futures) {
				  Array result = future.get(); 
//				  System.out.println(result);	 
			  }
			} catch (ExecutionException | InterruptedException ex) { }
			pool.shutdown();

		
		long endTime = System.currentTimeMillis();
		System.out.println("time consuming:    " + (endTime - startTime));
    	
    	
//    	long startTime = System.currentTimeMillis();
//        RandomAccessFile raf = new RandomAccessFile(path + "g512_v070_ergl_pres_h000.2018052212.gb2", "r");
//		Grib2RecordScanner scan = new Grib2RecordScanner(raf);
//		int count = 0;
//		while (scan.hasNext()) {
//			Grib2Record rec = scan.next();
//			Grib2SectionDataRepresentation drs = rec.getDataRepresentationSection();
//			float[] data = rec.readData(raf, drs.getStartingPosition());
//		}
//		long endTime = System.currentTimeMillis();
//		System.out.println("time consuming:    " + (endTime - startTime));
//		raf.close();
		
    }
}

class NwpCallable implements Callable<Array> {

	GridDataset dataset;
	float lon;
	float lat;
	String var;	
	
	NwpCallable(String absPath, float lon, float lat, String var) throws IOException {
		this.dataset = GridDataset.open(absPath);
		this.lon = lon;
		this.lat = lat;
		this.var = var;
	}

	public Array call() throws IOException {
		long t1 = System.currentTimeMillis();
		
    	GeoGrid grid = dataset.findGridByName("Temperature_isobaric");
    	GeoGrid grid2 = dataset.findGridByName("Relative_humidity_isobaric");
    	GeoGrid grid3 = dataset.findGridByName("u-component_of_wind_isobaric");
    	GeoGrid grid4 = dataset.findGridByName("v-component_of_wind_isobaric");
    	GridCoordSystem xyAxis = grid.getCoordinateSystem();
    	int[] yxIndex = xyAxis.findXYindexFromLatLon(38,126,null);
    	Array dathum = grid.readDataSlice(0, -1, yxIndex[1], yxIndex[0]);
    	Array dathum2 = grid2.readDataSlice(0, -1, yxIndex[1], yxIndex[0]);
    	Array dathum3 = grid3.readDataSlice(0, -1, yxIndex[1], yxIndex[0]);
    	Array dathum4 = grid4.readDataSlice(0, -1, yxIndex[1], yxIndex[0]);

		long t2 = System.currentTimeMillis();
		System.out.println("time consuming for each file:    " + (t2 - t1));
    	
	    return dathum;		
	  }		  	
}
