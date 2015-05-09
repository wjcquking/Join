package org.macau.local.analysis.time;

import java.util.Stack;

/**
 * JAVA获得一个数组的指定长度的排列组合。<br>
 * 
 * 
 * @author JAVA世纪网(java2000.net, laozizhu.com)
 */
public class OneFeatureJoin {
	
  public static void main(String[] args) {
	  OneFeatureJoin t = new OneFeatureJoin();
    Object[] arr = { 1, 2, 3 };
    // 循环获得每个长度的排列组合
    for (int num = 1; num <= arr.length; num++) {
      t.getSequence(arr, 0, num);
      
    }
  }

  // 存储结果的堆栈
  private Stack<Object> stack = new Stack<Object>();

  /**
   * 获得指定数组从指定开始的指定数量的数据组合<br>
   * 
   * @param arr 指定的数组
   * @param begin 开始位置
   * @param num 获得的数量
   */
  public void getSequence(Object[] arr, int begin, int num) {
    if (num == 0) {
      System.out.println(stack); // 找到一个结果
    } else {
      // 循环每个可用的元素
      for (int i = begin; i < arr.length; i++) {
        // 当前位置数据放入结果堆栈
        stack.push(arr[i]);
        // 将当前数据与起始位置数据交换
        swap(arr, begin, i);
        // 从下一个位置查找其余的组合
        getSequence(arr, begin + 1, num - 1);
        // 交换回来
        swap(arr, begin, i);
        // 去除当前数据
        stack.pop();
      }
    }
  }

  /**
   * 交换2个数组的元素
   * 
   * @param arr 数组
   * @param from 位置1
   * @param to 位置2
   */
  public static void swap(Object[] arr, int from, int to) {
    if (from == to) {
      return;
    }
    Object tmp = arr[from];
    arr[from] = arr[to];
    arr[to] = tmp;
  }
}
