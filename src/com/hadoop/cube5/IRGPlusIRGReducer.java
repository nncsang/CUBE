package com.hadoop.cube5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.cube.TimeStampWritable;
import com.hadoop.cube.data_structure.RollUp;
import com.hadoop.cube.settings.GlobalSettings;

public class IRGPlusIRGReducer extends Reducer<TupleWritable5,
											LongWritable, 
											TimeStampWritable, 
											LongWritable> { 

	protected TimeStampWritable previousKey;
	protected long[] tmpRes;
	protected TimeStampWritable previousKey2;
	protected long[] tmpRes2;
	protected int pivot;
	protected boolean secondPass;

	private Map<String, Integer> currentAttributePosition;
	
	private List<RollUp> rollups;
	private RollUp currentRollUp;
	
	private List<Integer> currentRegionList;
	private int numRegion;
	
	private int beginRightIndex;
	private int beginLeftIndex;
	private int lastLeftIndex;
	private int lastRightIndex;
	
	private int lastRightGroupingIndex;
	private int lastLeftGroupingIndex;
	
	private String[] attributes;
	
	private int previousRollupId = -1;
	
	private boolean isNeedComputeAllRegion = false;
	
	private LongWritable tmpLW;
	
	private TimeStampWritable official;
	
	public IRGPlusIRGReducer() {
		previousKey = null;
		tmpRes = new long[TimeStampWritable.length];
		previousKey2 = null;
		tmpRes2 = new long[TimeStampWritable.length];
		secondPass = false;
		
		this.currentAttributePosition = new HashMap<String, Integer>();
		this.tmpLW = new LongWritable();
		this.official = new TimeStampWritable();
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		computeLastRegion(context);
		super.cleanup(context);
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		
        String[] regionStringList = conf.get("regionList").split(GlobalSettings.DELIM_BETWEEN_ROLLUPS);
        String[] rollupListString = conf.get("rollupList").split(GlobalSettings.DELIM_BETWEEN_ROLLUPS);
        
        this.attributes = conf.get("attributes").split(GlobalSettings.DELIM_BETWEEN_ATTRIBUTES);
   
		this.rollups = new ArrayList<RollUp>();
		
		for(int i = 0; i < rollupListString.length; i++){
			RollUp rollup = new RollUp(rollupListString[i].split(GlobalSettings.DELIM_BETWEEN_ATTRIBUTES));
			
			String[] regionIds = regionStringList[i].split(GlobalSettings.DELIM_BETWEEN_GROUPIDS);
			for(int j = 0; j < regionIds.length; j++){
				rollup.enabledRegions.add(Integer.parseInt(regionIds[j]));
        	}
			
			this.rollups.add(rollup);
		}
		
		this.reset();
		this.pivot = Integer.parseInt(context.getConfiguration().get("hybrid.pivot", "-1"));
	}
	
	public void reset(){
		
		previousKey = new TimeStampWritable(null);
		previousKey2 = new TimeStampWritable(null);
		for (int i = 0; i < TimeStampWritable.length; i++) {
		    tmpRes[i] = 0;
		    tmpRes2[i] = 0;
		    
		}
		secondPass = false;
	}
	
	protected void new_compute(TimeStampWritable key, LongWritable value, Context context)
		    throws IOException, InterruptedException {
			
			if (previousKey.fields != null) {
			    LongWritable tmpVal = new LongWritable();
			    for(int index = 0; index <= lastRightIndex; index++){
			    	if (previousKey.fields[index] != key.fields[index]){
			    		int x = Math.max(index, beginRightIndex);
			    		for(int j = lastRightGroupingIndex - 1; j >= x ; j--){
			    			tmpRes[j] += tmpRes[j + 1];
			    			tmpVal.set(tmpRes[j + 1]);
			    			tmpRes[j + 1] = 0;
			    			previousKey.fields[j + 1] = TimeStampWritable.NullValue;
			    			writeResult(context, previousKey, tmpVal);
			    		}
			    		break;
			    	}
			    }
			}
			
			if (key.fields[0] != TimeStampWritable.NullValue) {
				
				if (lastRightIndex == TimeStampWritable.length - 1)
					context.write(key, value);
				
				for(int i = TimeStampWritable.length - 1; i >= lastRightGroupingIndex; i--)
					key.fields[i] = TimeStampWritable.NullValue;
				
				tmpRes[lastRightGroupingIndex] += value.get();
			    previousKey.fields = key.fields.clone();
			}
		}
	
	protected void new_compute2(TimeStampWritable key, LongWritable value, Context context)
		    throws IOException, InterruptedException {
		
		if (previousKey2.fields != null) {
		    LongWritable tmpVal = new LongWritable();
		    for (int i = 0; i <= lastLeftIndex; i++) {
		    	int x = Math.max(i, beginLeftIndex);
		        if (previousKey2.fields[i] != key.fields[i]) {
		        	for (int j = lastLeftGroupingIndex - 1; j >= x; j--) {
		                tmpRes2[j] += tmpRes2[j+1];
		                tmpVal.set(tmpRes2[j+1]);
		                tmpRes2[j+1] = 0;
		                previousKey2.fields[j+1] = TimeStampWritable.NullValue;
		                writeResult(context, previousKey2, tmpVal);
		            }
		            break;
		        }
		    }
		}
		
		if (key.fields[0] != TimeStampWritable.NullValue) {
			if (lastLeftIndex == pivot - 1)
				writeResult(context, key, value);
			
			for(int i = pivot - 1; i >= lastLeftGroupingIndex; i--)
				key.fields[i] = TimeStampWritable.NullValue;
			
			tmpRes2[lastLeftGroupingIndex] += value.get();
			
		    previousKey2.fields = key.fields.clone();
		    
		} else if (pivot == 0) {
			writeResult(context, key, value);
		}
	}
	
	
	public void writeResult(Context context, TimeStampWritable key, LongWritable value) throws IOException, InterruptedException{		
		int length = this.attributes.length;
		
		for(int i = 0; i < length; i++){
			this.official.fields[i] = key.fields[this.currentAttributePosition.get(this.attributes[i])];
		}
		
		context.write(this.official, value);
	}
	
	public void setAttributePosition(){
		this.currentRollUp = rollups.get(previousRollupId);
		this.currentRegionList = this.currentRollUp.enabledRegions;
		this.numRegion = this.currentRegionList.size();
		
		String[] attributes = this.currentRollUp.getAttributes();
		int length = attributes.length;
		
		for(int i = 0; i < length; i++){
			currentAttributePosition.put(attributes[i], i);
		}
		
		this.lastLeftIndex = -1;
		this.lastRightIndex = -1;
		
		for(int i = this.numRegion - 1; i >= 0; i--){
			int index = this.currentRegionList.get(i) - 1;
			if (index < pivot && lastLeftIndex == -1){
				lastLeftIndex = index;
			}
			
			if (index >= pivot && lastRightIndex == -1){
				lastRightIndex = index;
			}
		}
		
		this.beginLeftIndex = -2;
		this.beginRightIndex = -1;
		
		for(int i = 0; i < this.numRegion; i++){
			int index = this.currentRegionList.get(i) - 1;
			if (index < pivot && this.beginLeftIndex == -2){
				beginLeftIndex = index;
			}
			
			if (index >= pivot && beginRightIndex == -1){
				beginRightIndex = index;
			}
		}
		
		if (this.beginLeftIndex == -1)
			this.beginLeftIndex = 0;
		
		if (this.currentRegionList.get(0) == 0)
			this.isNeedComputeAllRegion = true;
		else
			this.isNeedComputeAllRegion = false;
		
		if (this.lastRightIndex == TimeStampWritable.length - 1)
			lastRightGroupingIndex = lastRightIndex;
		else
			lastRightGroupingIndex = lastRightIndex + 1;
		
		if (this.lastLeftIndex == pivot - 1)
			lastLeftGroupingIndex = pivot - 1;
		else
			lastLeftGroupingIndex = lastLeftIndex + 1;
	}
	
	@Override
	protected void reduce(TupleWritable5 tuple, Iterable<LongWritable> value, Context context)
		throws IOException, InterruptedException {
		
    	TimeStampWritable key = tuple.timeStampWritable;    	
    	int rollupID = tuple.id;
    	
    	if (this.previousRollupId == -1){
    		this.previousRollupId = rollupID;
    		setAttributePosition();
    	}
    	
    	if (this.previousRollupId != rollupID){	
    		computeLastRegion(context);
	    	this.previousRollupId = rollupID;
	    	this.setAttributePosition();
	    	this.reset();
    	}
    	
    	long sum = 0;
		for (LongWritable lw : value) {
			sum += lw.get();
		}
		
		this.tmpLW.set(sum);
		if (key.fields[pivot] != TimeStampWritable.NullValue) {
			new_compute(key, tmpLW, context);
		} else {
			secondPass = true;
			new_compute2(key, tmpLW, context);
		}
	}
	
	public void computeLastRegion(Context context) throws IOException, InterruptedException{
		int x = TimeStampWritable.NullValue;
		
		this.tmpLW.set(0);
    	new_compute(new TimeStampWritable(x, x, x, x, x), this.tmpLW, context);
    		
    	if (secondPass && (pivot != 0)) {
    		new_compute2(new TimeStampWritable(x, x, x, x, x), this.tmpLW, context);
    		
    		if (this.isNeedComputeAllRegion){
    			this.tmpLW.set(tmpRes2[0]);
    		    context.write(new TimeStampWritable(x, x, x, x, x), this.tmpLW);
    		}
    	}
	}
}
