/**
 * Copyright contributors to the db2-accelerator-data-comparison project
 */
package com.ibm.db2.accelerator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Minimal log infrastructure
 */
public class Logger {

	private static Logger gLogger;
	private boolean debugMode = false;
	
	public boolean isDebugMode() {
		return debugMode;
	}

	public void setDebugMode(boolean debugMode) {
		this.debugMode = debugMode;
	}

	public static Logger getInstance() {
		if (gLogger == null) {
			gLogger = new Logger();
		}
		return gLogger;
	}

	public void addLogEntry(String logEntry) {
		System.out.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + " " + logEntry);
	}
	
	public void addLogEntryDebug(String logEntry) {
		if (debugMode) {
			addLogEntry(logEntry);
		}
	}
	
}
