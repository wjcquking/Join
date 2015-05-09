package org.macau.flickr.knn.exact.preprocessing.kmeans;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.macau.spatial.Distance;

/**
 * 
 * @author Yuanbo She
 * 
 */
public class kmeans {

    /**
     * double[][] the data
     * 
     * @param matrix
     *            double[][]
     * @param highDim
     *            int
     * @param lowDim
     *            int <br/>
     *            double[highDim][lowDim]
     */
    private static void setDouble2Zero(double[][] matrix, int highDim, int lowDim) {
        for (int i = 0; i < highDim; i++) {
            for (int j = 0; j < lowDim; j++) {
                matrix[i][j] = 0;
            }
        }
    }

    /**
     * 锟斤拷锟斤拷源锟斤拷维锟斤拷锟斤拷元锟截碉拷目锟斤拷锟轿�拷锟斤拷锟�foreach (dests[highDim][lowDim] = sources[highDim][lowDim]);
     * 
     * @param dests
     *            double[][]
     * @param sources
     *            double[][]
     * @param highDim
     *            int
     * @param lowDim
     *            int
     */
    private static void copyCenters(double[][] dests, double[][] sources, int highDim, int lowDim) {
        for (int i = 0; i < highDim; i++) {
            for (int j = 0; j < lowDim; j++) {
                dests[i][j] = sources[i][j];
            }
        }
    }

    /**
     * 锟斤拷锟铰撅拷锟斤拷锟斤拷锟斤拷锟斤拷锟�
     * 
     * @param k
     *            int 锟斤拷锟斤拷锟斤拷锟�
     * @param data
     *            kmeans_data
     */
    private static void updateCenters(int k, kmeans_data data) {
        double[][] centers = data.centers;
        setDouble2Zero(centers, k, data.dim);
        int[] labels = data.labels;
        int[] centerCounts = data.centerCounts;
        for (int i = 0; i < data.dim; i++) {
            for (int j = 0; j < data.length; j++) {
                centers[labels[j]][i] += data.data[j][i];
            }
        }
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < data.dim; j++) {
                centers[i][j] = centers[i][j] / centerCounts[i];
            }
        }
    }

    /**
     * calculate the distance of two point
     * 
     * @param pa
     *            double[]
     * @param pb
     *            double[]
     * @param dim
     *            int dimension
     * @return double distance
     */
    public static double dist(double[] pa, double[] pb, int dim) {
    	
    	return Distance.GreatCircleDistance(pa[0], pa[1], pb[0],pb[1]);
    	
    	/*
        double rv = 0;
        for (int i = 0; i < dim; i++) {
            double temp = pa[i] - pb[i];
            temp = temp * temp;
            rv += temp;
        }
        return Math.sqrt(rv);
        */
    }

    /**
     * 锟斤拷Kmeans锟斤拷锟斤拷
     * 
     * @param k
     *            int 锟斤拷锟斤拷锟斤拷锟�
     * @param data
     *            kmeans_data kmeans锟斤拷锟斤拷锟�
     * @param param
     *            kmeans_param kmeans锟斤拷锟斤拷锟斤拷
     * @return kmeans_result kmeans锟斤拷锟斤拷锟斤拷息锟斤拷
     */
    public static kmeans_result doKmeans(int k, kmeans_data data, kmeans_param param) {
        // 预锟斤拷锟斤拷
        double[][] centers = new double[k][data.dim]; // 锟斤拷锟斤拷锟斤拷锟侥点集
        data.centers = centers;
        int[] centerCounts = new int[k]; // 锟斤拷锟斤拷锟斤拷陌锟斤拷锟斤拷锟�
        data.centerCounts = centerCounts;
        Arrays.fill(centerCounts, 0);
        int[] labels = new int[data.length]; // 锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷
        data.labels = labels;
        double[][] oldCenters = new double[k][data.dim]; // 锟斤拷时锟斤拷锟斤拷傻木锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷

        // 锟斤拷始锟斤拷锟斤拷锟斤拷锟斤拷锟侥ｏ拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷选锟斤拷data锟节碉拷k锟斤拷锟斤拷锟截革拷锟姐）
        if (param.initCenterMehtod == kmeans_param.CENTER_RANDOM) { //choose k initial cluster center
            Random rn = new Random();
            List<Integer> seeds = new LinkedList<Integer>();
            while (seeds.size() < k) {
                int randomInt = rn.nextInt(data.length);
                if (!seeds.contains(randomInt)) {
                    seeds.add(randomInt);
                }
            }
            Collections.sort(seeds);
            for (int i = 0; i < k; i++) {
                int m = seeds.remove(0);
                for (int j = 0; j < data.dim; j++) {
                    centers[i][j] = data.data[m][j];
                }
            }
        } else { // 选取前k锟斤拷锟斤拷位锟斤拷始锟斤拷锟斤拷锟斤拷锟斤拷
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < data.dim; j++) {
                    centers[i][j] = data.data[i][j];
                }
            }
        }

        // 锟斤拷一锟街碉拷锟�
        for (int i = 0; i < data.length; i++) {
            double minDist = dist(data.data[i], centers[0], data.dim);
            int label = 0;
            for (int j = 1; j < k; j++) {
                double tempDist = dist(data.data[i], centers[j], data.dim);
                if (tempDist < minDist) {
                    minDist = tempDist;
                    label = j;
                }
            }
            labels[i] = label;
            centerCounts[label]++;
        }
        updateCenters(k, data);
        copyCenters(oldCenters, centers, k, data.dim);

        // 锟斤拷锟皆わ拷锟斤拷锟�
        int maxAttempts = param.attempts > 0 ? param.attempts : kmeans_param.MAX_ATTEMPTS;
        int attempts = 1;
        double criteria = param.criteria > 0 ? param.criteria : kmeans_param.MIN_CRITERIA;
        double criteriaBreakCondition = 0;
        boolean[] flags = new boolean[k]; // 锟斤拷锟斤拷锟叫╋拷锟斤拷谋锟斤拷薷墓锟�

        // iteration
        //the number of iteration is less than the maxAttempts and the change of center point is less than the criteria which is the threshold 
        iterate: while (attempts < maxAttempts) {
        	
            for (int i = 0; i < k; i++) { // 锟斤拷始锟斤拷锟斤拷锟侥点“锟角凤拷锟睫改癸拷锟斤拷
                flags[i] = false;
            }
            for (int i = 0; i < data.length; i++) { // 锟斤拷锟斤拷data锟斤拷锟斤拷锟叫碉拷
                double minDist = dist(data.data[i], centers[0], data.dim);
                int label = 0;
                for (int j = 1; j < k; j++) {
                    double tempDist = dist(data.data[i], centers[j], data.dim);
                    if (tempDist < minDist) {
                        minDist = tempDist;
                        label = j;
                    }
                }
                if (label != labels[i]) { // 锟斤拷锟角帮拷惚伙拷锟斤拷嗟斤拷碌锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷
                    int oldLabel = labels[i];
                    labels[i] = label;
                    centerCounts[oldLabel]--;
                    centerCounts[label]++;
                    flags[oldLabel] = true;
                    flags[label] = true;
                }
            }
            updateCenters(k, data);
            attempts++;

            // 计算中心值点对最大修改量是否超过threshold
            double maxDist = 0;
            for (int i = 0; i < k; i++) {
                if (flags[i]) {
                    double tempDist = dist(centers[i], oldCenters[i], data.dim);
                    if (maxDist < tempDist) {
                        maxDist = tempDist;
                    }
                    for (int j = 0; j < data.dim; j++) { // update the oldCenter
                        oldCenters[i][j] = centers[i][j];
                    }
                }
            }
            if (maxDist < criteria) {
            	System.out.println("dist " + criteriaBreakCondition);
                criteriaBreakCondition = maxDist;
                break iterate;
            }
        }

        // output the information
        kmeans_result rvInfo = new kmeans_result();
        rvInfo.attempts = attempts;
        rvInfo.criteriaBreakCondition = criteriaBreakCondition;
        if (param.isDisplay) {
            System.out.println("k=" + k);
            System.out.println("attempts=" + attempts);
            System.out.println("criteriaBreakCondition=" + criteriaBreakCondition);
            System.out.println("The number of each classes are: ");
            for (int i = 0; i < k; i++) {
                System.out.print(centerCounts[i] + " ");
            }
            System.out.print("\n\n");
        }
        return rvInfo;
    }
}
