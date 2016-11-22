package org.voltdb.repartitioner;

import java.util.ArrayList;
import java.util.Collections;

public class StatsList {

	/**
	 * List of statistics.
	 */
	private ArrayList<Integer> m_list = new ArrayList<Integer>();
	
	/**
	 * Median of the list.
	 */
	private Integer m_median;	
	
	public void add(Integer stat)
	{
		m_list.add(stat);
		
		if(m_median != null)
		{
			m_median = null;
		}
	}
	
	public Integer getMedian()
	{
		assert(!m_list.isEmpty());
		
		if(m_median == null)
		{
			int numStats = m_list.size();
			
			if (numStats > 1) {
				Collections.sort(m_list);

				if (numStats % 2 != 0) {
					m_median = m_list.get(numStats / 2);
				} else {
					m_median = Math.round((float) (m_list.get(numStats / 2 - 1) + m_list.get(numStats / 2)) / 2);
				}
			} else {
				m_median = m_list.get(0);
			}
		}
		
		return m_median;
	}
	
	public void clear()
	{
		m_list.clear();
		m_median = null;
	}
	
}
