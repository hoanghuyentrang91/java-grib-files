package kara.grib;

import java.io.IOException;
import java.util.Arrays;

import ucar.ma2.Array;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDataset;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.grib.grib2.Grib2Record;
import ucar.nc2.grib.grib2.Grib2RecordScanner;
import ucar.nc2.grib.grib2.Grib2SectionDataRepresentation;
import ucar.unidata.io.RandomAccessFile;

public class GribTest 
{
	private final static String path = "C:/Users/SNS/Desktop/KARA/WFH/Datafiles/DATA/NWP/UMGL/201711/09/";
    public static void main( String[] args ) throws IOException 
    {

        RandomAccessFile raf = new RandomAccessFile(path + "g512_v070_ergl_pres_h000.2017110900.gb2", "r");
		boolean res = Grib2RecordScanner.isValidFile(raf);
		System.out.println(res);
		Grib2RecordScanner scan = new Grib2RecordScanner(raf);
		int count = 0;
		while (scan.hasNext()) {
			Grib2Record rec = scan.next();
			System.out.println(rec.toString());
			System.out.println(++count);
			Grib2SectionDataRepresentation drs = rec.getDataRepresentationSection();
			float[] data = rec.readData(raf, drs.getStartingPosition());
		}
		
    }
}
