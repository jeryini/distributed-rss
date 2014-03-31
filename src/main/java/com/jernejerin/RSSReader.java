/**
 * 
 */
package com.jernejerin;

import com.sun.syndication.io.SyndFeedInput;

/**
 * @author Jernej Jerin
 *
 */
public class RSSReader implements Runnable {
	private String url;
	
	public RSSReader(String url) {
		this.url = url;
	}

	public void run() {
		try {
			// our input reader for rss
			SyndFeedInput input = new SyndFeedInput();
			
			while (true) {
				System.out.println(url);
				
				// check every second for new feeds
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
