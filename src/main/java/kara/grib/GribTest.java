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
    	int stepsNum = 5;
    	int bandNum = 19;
    	String firstDataFile = fullPath.substring(fullPath.lastIndexOf("/") + 1);
		String currentStep = firstDataFile.substring(21,24);
		String timeStep;
		String var = "Temperature_isobaric";
		float lon = 126.0f;
		float lat = 38.0f;
		
		float[][] data = new float[bandNum][stepsNum];
		
		final List<Callable<float[][]>> callables = new ArrayList<>();
		final ExecutorService pool = Executors.newFixedThreadPool(stepsNum);
		
		for (int iTime = 0; iTime < stepsNum; iTime++) {
	    	timeStep = String.format("%03d", (Integer.parseInt(currentStep) + 3 * iTime));
			String file =  firstDataFile.replace(("h" + currentStep), ("h" + timeStep));
			String absPath = fullPath.replace(firstDataFile, file);
			
			// Using GridDataset
//			callables.add(new NwpCallable(absPath, lon, lat, var));
			
			// Using RAF
			callables.add(new NwpCallable2(absPath, lon, lat, var));
		}
		
		List<Future<float[][]>> futures = pool.invokeAll(callables);
		try {
			  for (final Future<float[][]> future : futures) {
				  float[][] result = future.get(); 
				  System.out.println(result[2][0]);	 
			  }
			} catch (ExecutionException | InterruptedException ex) { }
			pool.shutdown();

		
		long endTime = System.currentTimeMillis();
		System.out.println("time consuming:    " + (endTime - startTime));  	
    	
    }
}


class NwpCallable2 implements Callable<float[][]> {

	String absPath;
	float lon;
	float lat;
	String var;	
	
	NwpCallable2(String absPath, float lon, float lat, String var) throws IOException {
		this.absPath = absPath;
		this.lon = lon;
		this.lat = lat;
		this.var = var;
	}

	public float[][] call() throws IOException {
		RandomAccessFile raf = new RandomAccessFile(absPath, "r");
		Grib2RecordScanner scan = new Grib2RecordScanner(raf);
		int count = 0;
		float[][] data = new float[4][19];
		while (scan.hasNext()) {
			count++;
			Grib2Record rec = scan.next();
			if (count >= 27 && count < 46) {		
				float[] uData = rec.readData(raf);
				System.out.println(uData.length);
				data[0] = uData;
			}
			if (count >= 53 && count < 72) {			
				float[] vData = rec.readData(raf);
				data[1] = vData;
			}
			if (count >= 105 && count < 124) {			
				float[] tmpData = rec.readData(raf);
				data[2] = tmpData;
			}
			if (count >= 153 && count < 172) {			
				float[] rhData = rec.readData(raf);
				data[3] = rhData;
			}
		}
		raf.close();
		return data;

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
