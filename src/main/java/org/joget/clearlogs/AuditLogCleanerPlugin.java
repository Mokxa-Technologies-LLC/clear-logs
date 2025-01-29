package org.joget.clearlogs;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

import org.joget.apps.app.model.AppDefinition;
import org.joget.apps.app.service.AppUtil;
import org.joget.plugin.base.DefaultApplicationPlugin;
import org.joget.workflow.model.WorkflowAssignment;
import org.joget.workflow.model.service.WorkflowManager;
import org.joget.commons.util.LogUtil;
import org.joget.commons.util.SetupManager;

public class AuditLogCleanerPlugin extends DefaultApplicationPlugin {

    @Override
    public String getName() {
        return "Log Cleaner Plugin";
    }

    @Override
    public String getVersion() {
        return "8.0.0";
    }

    @Override
    public String getDescription() {
        return "A plugin to remove audit logs for API Builder and Scheduler with configurable options.";
    }

    @Override
    public Object execute(Map properties) {
    	WorkflowManager wfm = AppUtil.getApplicationContext().getBean("workflowManager", WorkflowManager.class);
		WorkflowAssignment wfAssignment = (WorkflowAssignment) properties.get("workflowAssignment");
		String logType = getPropertyString("logType"); //WorkflowUtil.processVariable((String) properties.get("gitRepositoryURL"), "", wfAssignment);
        String duration = getPropertyString("duration"); //WorkflowUtil.processVariable((String) properties.get("operationName"), "", wfAssignment);
        LogUtil.info("Logtype", logType);
        LogUtil.info("duration", duration);
        DataSource ds = (DataSource) AppUtil.getApplicationContext().getBean("setupDataSource");
        
        try (Connection con = ds.getConnection()) {
        	Map<String, String> map = determineLogTable(logType);
        	String tableName = null;
        	String columnName = null;
        	if (map != null && !map.isEmpty()) {
        	    Map.Entry<String, String> entry = map.entrySet().iterator().next();
        	    tableName = entry.getKey();  
        	    columnName = entry.getValue(); 
        	    deleteLogs(con, tableName,columnName, Integer.parseInt(duration));
        	}
        } catch (SQLException e) {
            LogUtil.error(getClass().getName(), e, "Error while cleaning audit logs.");
        }

        return null;
    }

    private Map<String, String> determineLogTable(String logType) {
    	Map<String, String> map =  new HashMap();
        switch (logType) {
            case "API":
                map.put("api_log", "timestamp");
                break;
            case "Scheduler":
                map.put("sch_job_log" , "startTime");
                break;
            case "Audit":
                 map.put("wf_audit_trail", "timestamp");
                 break;
            case "All":
            	clearTomcatLogs();
                break;
            default:
            	LogUtil.info("Inside the default", logType);
                map.put(null, null);
        }
        return map;
    }

    private void deleteLogs(Connection con, String tableName, String columnName, int duration) throws SQLException {

    	String sql = "SELECT * FROM " + tableName + " WHERE " + columnName + " < NOW() - INTERVAL ? DAY";
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, duration);  
            int rows = ps.executeUpdate(); 
            LogUtil.info(getClass().getName(), rows + " rows will be deleted from " + tableName);
            
            int rowsDeleted;
            do {
                
                String sql1 = "DELETE FROM " + tableName + " WHERE " + columnName + " < NOW() - INTERVAL ? DAY LIMIT 100";
                try (PreparedStatement ps1 = con.prepareStatement(sql1)) {
                    ps1.setInt(1, duration);  
                    rowsDeleted = ps1.executeUpdate();  
                    LogUtil.info(getClass().getName(), rowsDeleted + " rows deleted from " + tableName);
                }
            } while (rowsDeleted > 0);
        }
    }


    @Override
    public String getLabel() {
        return "Log Cleaner";
    }

    @Override
    public String getPropertyOptions() {
    	AppDefinition appDef = AppUtil.getCurrentAppDefinition();
	    String appId = appDef.getId();
	    String appVersion = appDef.getVersion().toString();
	    Object[] arguments = new Object[]{appId, appVersion};
        return AppUtil.readPluginResource(getClass().getName(), "/properties/logCleaner.json", null, true, null);
    }

	@Override
	public String getClassName() {
		// TODO Auto-generated method stub
		return this.getClassName();
	}
	
	private void clearTomcatLogs() {
        try {
        	String baseDir = SetupManager.getBaseSharedDirectory();
            LogUtil.info("BaseDir", baseDir);
            Path currentPath = Paths.get(baseDir); 
            Path apacheTomcatPath = currentPath.getParent().resolve("apache-tomcat");
            System.out.println(apacheTomcatPath.toString()); 
            
        	 FilenameFilter tomcatDirFilter = new FilenameFilter() {
                 public boolean accept(File dir, String name) {
                     return name.startsWith("apache-tomcat-"); 
                 }
             };
             File baseDirectory = new File(baseDir);

             File[] tomcatDirs = baseDirectory.listFiles(tomcatDirFilter);

             if (tomcatDirs != null && tomcatDirs.length > 0) {
                 File tomcatDir = tomcatDirs[0]; 

                 File logDir = new File(tomcatDir, "logs");
                 if (logDir.exists() && logDir.isDirectory()) {
                     File[] logFiles = logDir.listFiles();
                     if (logFiles != null) {
                         for (File file : logFiles) {
                             if (file.isFile()) {
                                 boolean deleted = file.delete();
                                 if (deleted) {
                                     LogUtil.info(this.getClassName(), "Deleted log file: " + file.getName());
                                 } else {
                                     LogUtil.warn(this.getClassName(), "Failed to delete log file: " + file.getName());
                                 }
                             }
                         }
                     }
                 } else {
                     LogUtil.warn(this.getClass().getName(), "Tomcat logs directory not found: " + logDir.getAbsolutePath());
                 }
             } else {
                 LogUtil.warn(this.getClass().getName(), "No Tomcat directory found with the pattern 'apache-tomcat-*'.");
             }

         } catch (Exception e) {
             LogUtil.error(this.getClass().getName(), e, "Error while clearing Tomcat log files.");
         }
     }
 }