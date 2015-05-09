package org.macau;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.macau.join.phase1.BasicTokenOrdering;
import org.macau.join.OnePhaseTokenOrdering;
import org.macau.join.phase2.BasicKernel;
import org.macau.join.phase3.OnePhaseRecordJoin;
import org.macau.token.generator.RecordBuild;
import org.macau.util.SimilarityUtil;

public class WordCount2 {
	
	
	// if the output path exist, then delete it
	public static void cleanFiles(Configuration conf) throws Exception{
		FileSystem.get(conf).delete( new Path(SimilarityUtil.recordsGeneratePath), true);
		FileSystem.get(conf).delete( new Path(SimilarityUtil.tokenOutputPath), true);
		FileSystem.get(conf).delete( new Path(SimilarityUtil.ridPairsOutputPath), true);
		FileSystem.get(conf).delete( new Path(SimilarityUtil.tokenPhase1Path), true);
		FileSystem.get(conf).delete( new Path(SimilarityUtil.finalPairsOutputPath), true);
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		
		conf.addResource(new Path("conf/core-site.xml"));
		cleanFiles(conf);
		
		
		  
		Date startTime = new Date();
		// data generator
		System.out.println("record build");
		boolean recordsBuild = RecordBuild.RecordsBuild(conf);
		Date recordsBuildTime = new Date();
		
		
			
		System.out.println("Phase One Start"+ System.currentTimeMillis());
		
		boolean phaseOneState = BasicTokenOrdering.BasicTokenOrder(conf);
		//boolean phaseOneState = OnePhaseTokenOrdering.OnePhaseTokenOrder(conf);
		
		
		//read the original data again and sort the textual value according the global order
		if(phaseOneState){
			
			Date phaseOneTime = new Date();
			
			System.out.println("Phase One Finished");
			System.out.println("Phase Two Start");
			boolean phaseTwoState = BasicKernel.BasicKernelSpatialBasicJoin(conf);
			//the third job
			if(phaseTwoState){
				Date phaseTwoTime = new Date();
				
				System.out.println("Phase Two Finished");
				System.out.println("Phase Three Start");
				boolean phaseThreeState =OnePhaseRecordJoin.OnePahseRecordJoinJob(conf);
				if(phaseThreeState){
					System.out.println("Phase Three Finished");
				}
				Date end_time = new Date();
				
				
				System.out.println("recordBuild time is: " + (recordsBuildTime.getTime() - startTime.getTime())/ (float) 1000.0 + " seconds.");
				System.out.println("Pahse One time is: " + (phaseOneTime.getTime() - recordsBuildTime.getTime())/ (float) 1000.0 + " seconds.");
				System.out.println("Pahse Two time is: " + (phaseTwoTime.getTime() - phaseOneTime.getTime())/ (float) 1000.0 + " seconds.");
				System.out.println("Pahse Three time is: " + (end_time.getTime() - phaseTwoTime.getTime())/ (float) 1000.0 + " seconds.");
		        System.out.println("Complete-Job ended: " + end_time);
		        System.out.println("The complete-job took "
		                + (end_time.getTime() - startTime.getTime()) / (float) 1000.0
		                + " seconds.");
				System.exit(phaseThreeState ? 0 : 1);
			}
			
			
		}
				

	}
}
