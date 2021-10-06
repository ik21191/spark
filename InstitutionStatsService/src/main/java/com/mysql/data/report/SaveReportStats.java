package com.mysql.data.report;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import com.mps.commons.AppConfig;
import com.mps.enums.MongoConfig;
import com.mps.stats.beans.IrMaster;
import com.mps.stats.beans.PrMaster;
import com.mps.stats.beans.TrMaster;
import com.mps.utils.MyDataTable;
import com.mps.utils.MyLogger;
import com.mysql.db.conf.ConnectionMaster;
import com.mysql.db.conf.MySqlConnConf;

public class SaveReportStats implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private static Connection con;
	private static MyDataTable mdt;
	private static String dynamicColumn;
	private static long totalDataToSave;

	public static boolean saveTRMaster(Set<TrMaster> trMasterSet) throws Exception {
		boolean status = false; 
		String mysqlTableName = AppConfig.get(MongoConfig.TR_TABLE_NAME.getValue());
		
		try {
			//get database connection
			con = ConnectionMaster.getMySqlConnection(new MySqlConnConf());
			// create the table
			mdt = new MyDataTable(mysqlTableName);
			// Add all the required column with default values
			addVirtualColumnTR(mdt);
			
			//fill the values in all above columns in the table
			MyLogger.log("SaveReportStats: save: for loop: Start");
			for(TrMaster trMaster : trMasterSet) {
				totalDataToSave++;
				mdt.addRow();
				int rowNo = mdt.getRowCount();
				mdt.updateData(rowNo, "Institution_ID", trMaster.getInstitutionID());
				mdt.updateData(rowNo, "Institution_Name", trMaster.getInstitutionName());
				mdt.updateData(rowNo, "Journal_ID", trMaster.getJournalID());
				mdt.updateData(rowNo, "Title", trMaster.getTitle());
				mdt.updateData(rowNo, "Publisher", trMaster.getPublisher());
				mdt.updateData(rowNo, "Publisher_ID", trMaster.getPublisherID());
				mdt.updateData(rowNo, "Platform", trMaster.getPlatform());
				mdt.updateData(rowNo, "DOI", trMaster.getDoi());
				mdt.updateData(rowNo, "Proprietary_ID", trMaster.getProprietaryID());
				mdt.updateData(rowNo, "ISBN", trMaster.getIsbn());
				mdt.updateData(rowNo, "Print_ISSN", trMaster.getPrintISSN());
				mdt.updateData(rowNo, "Online_ISSN", trMaster.getOnlineISSN());
				mdt.updateData(rowNo, "URI", trMaster.getUri());
				mdt.updateData(rowNo, "Data_Type", trMaster.getDataType());
				mdt.updateData(rowNo, "Section_Type", trMaster.getSectionType());
				mdt.updateData(rowNo, "YOP", trMaster.getYop());
				mdt.updateData(rowNo, "Access_Type", trMaster.getAccessType());
				mdt.updateData(rowNo, "Access_Method", trMaster.getAccessMethod());
				mdt.updateData(rowNo, "Metric_Type", trMaster.getMetricType());
				mdt.updateData(rowNo, "Reporting Period Total", Integer.toString(trMaster.getReportingPeriodTotal()));
				mdt.updateData(rowNo, "M_ERROR", "0");
				mdt.updateData(rowNo, getDynamicColumn(), Integer.toString(trMaster.getMetricCount()));
			}
			MyLogger.log("SaveReportStats: save: for loop: End");
			
			MyLogger.log("SaveReportStats: save: commit mdt Start: Total data to save: " + totalDataToSave + ", table: " + mysqlTableName + " , month: " + getDynamicColumn());
			totalDataToSave = 0;
			//save stats to mysql db
			MyLogger.log("SaveReportStats: save: commit mdt End, mdt.commit(con) result : " + mdt.commit(con));
			mdt.destroy();
			trMasterSet.clear();
			
			status = true;
		} catch(Exception e) {
			MyLogger.error("SaveReportStats: save: EXCEPTION: " + e);
			throw e;
		} finally {
			if(con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					MyLogger.error("SaveReportStats: save: problem while closing db connection: EXCEPTION: " + e);
				}
			}
		}
		return status;
	}
	
	public static boolean saveIRMaster(Set<IrMaster> irMasterSet) throws Exception {
		boolean status = false; 
		String mysqlTableName = AppConfig.get(MongoConfig.IR_TABLE_NAME.getValue());
		
		try {
			//get database connection
			con = ConnectionMaster.getMySqlConnection(new MySqlConnConf());
			// create the table
			mdt = new MyDataTable(mysqlTableName);
			// Add all the required column with default values
			addVirtualColumnIR(mdt);
			
			//fill the values in all above columns in the table
			MyLogger.log("SaveReportStats: saveIRMaster: for loop: Start");
			for(IrMaster irMaster : irMasterSet) {
				totalDataToSave++;
				mdt.addRow();
				int rowNo = mdt.getRowCount();
				mdt.updateData(rowNo, "Institution_ID", irMaster.getInstitutionID());
				mdt.updateData(rowNo, "Institution_Name", irMaster.getInstitutionName());
				mdt.updateData(rowNo, "Journal_ID", irMaster.getJournalID());
				mdt.updateData(rowNo, "Article_ID", irMaster.getArticleID());
				mdt.updateData(rowNo, "Item", irMaster.getItem());
				mdt.updateData(rowNo, "Publisher", irMaster.getPublisher());
				mdt.updateData(rowNo, "Publisher_ID", irMaster.getPublisherID());
				mdt.updateData(rowNo, "Platform", irMaster.getPlatform());
				mdt.updateData(rowNo, "Authors", irMaster.getAuthors());
				mdt.updateData(rowNo, "Publication_Date", irMaster.getPublicationDate());
				mdt.updateData(rowNo, "Article_Version", irMaster.getArticleVersion());
				mdt.updateData(rowNo, "DOI", irMaster.getDoi());
				mdt.updateData(rowNo, "Proprietary_ID", irMaster.getProprietaryID());
				mdt.updateData(rowNo, "ISBN", irMaster.getIsbn());
				mdt.updateData(rowNo, "Print_ISSN", irMaster.getPrintISSN());
				mdt.updateData(rowNo, "Online_ISSN", irMaster.getOnlineISSN());
				mdt.updateData(rowNo, "URI", irMaster.getUri());
				mdt.updateData(rowNo, "Parent_Title", irMaster.getParentTitle());
				mdt.updateData(rowNo, "Parent_Authors", irMaster.getParentAuthors());
				mdt.updateData(rowNo, "Parent_Publication_Date", irMaster.getParentPublicationDate());
				mdt.updateData(rowNo, "Parent_Article_Version", irMaster.getParentArticleVersion());
				mdt.updateData(rowNo, "Parent_Data_Type", irMaster.getParentDataType());
				mdt.updateData(rowNo, "Parent_DOI", irMaster.getParentDOI());
				mdt.updateData(rowNo, "Parent_Proprietary_ID", irMaster.getParentProprietaryID());
				mdt.updateData(rowNo, "Parent_ISBN", irMaster.getParentISBN());
				mdt.updateData(rowNo, "Parent_Print_ISSN", irMaster.getParentPrintISSN());
				mdt.updateData(rowNo, "Parent_Online_ISSN", irMaster.getParentOnlineISSN());
				mdt.updateData(rowNo, "Parent_URI", irMaster.getParentURI());
				mdt.updateData(rowNo, "Component_Title", irMaster.getComponentTitle());
				mdt.updateData(rowNo, "Component_Authors", irMaster.getComponentAuthors());
				mdt.updateData(rowNo, "Component_Article_Version", irMaster.getComponentArticleVersion());
				mdt.updateData(rowNo, "Component_Data_Type", irMaster.getComponentDataType());
				mdt.updateData(rowNo, "Component_DOI", irMaster.getComponentDOI());
				mdt.updateData(rowNo, "Component_Proprietary_ID", irMaster.getComponentProprietaryID());
				mdt.updateData(rowNo, "Component_ISBN", irMaster.getComponentISBN());
				mdt.updateData(rowNo, "Component_Print_ISSN", irMaster.getComponentPrintISSN());
				mdt.updateData(rowNo, "Component_Online_ISSN", irMaster.getComponentOnlineISSN());
				mdt.updateData(rowNo, "Component_URI", irMaster.getComponentURI());
				mdt.updateData(rowNo, "Data_Type", irMaster.getDataType());
				mdt.updateData(rowNo, "Section_Type", irMaster.getSectionType());
				mdt.updateData(rowNo, "YOP", irMaster.getYop());
				mdt.updateData(rowNo, "Access_Type", irMaster.getAccessType());
				mdt.updateData(rowNo, "Access_Method", irMaster.getAccessMethod());
				mdt.updateData(rowNo, "Metric_Type", irMaster.getMetricType());
				mdt.updateData(rowNo, "Reporting Period Total", Integer.toString(irMaster.getReportingPeriodTotal()));
				mdt.updateData(rowNo, "M_ERROR", "0");
				mdt.updateData(rowNo, getDynamicColumn(), Integer.toString(irMaster.getMetricCount()));
			}
			MyLogger.log("SaveReportStats: saveIRMaster: for loop: End");
			
			MyLogger.log("SaveReportStats: saveIRMaster: commit mdt Start: Total data to save: " + totalDataToSave + ", table: " + mysqlTableName + " , month: " + getDynamicColumn());
			totalDataToSave = 0;
			//save stats to mysql db
			MyLogger.log("SaveReportStats: saveIRMaster: commit mdt End, mdt.commit(con) result : " + mdt.commit(con));
			mdt.destroy();
			irMasterSet.clear();
			
			status = true;
		} catch(Exception e) {
			MyLogger.error("SaveReportStats: saveIRMaster: EXCEPTION: " + e);
			throw e;
		} finally {
			if(con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					MyLogger.error("SaveReportStats: saveIRMaster: problem while closing db connection: EXCEPTION: " + e);
				}
			}
		}
		return status;
	}
	
	public static boolean savePRMaster(Set<PrMaster> prMasterSet) throws Exception {
		boolean status = false;
		String mysqlTableName = AppConfig.get(MongoConfig.PR_TABLE_NAME.getValue());
		
		try {
			//get database connection
			con = ConnectionMaster.getMySqlConnection(new MySqlConnConf());
			// create the table
			mdt = new MyDataTable(mysqlTableName);
			// Add all the required column with default values
			addVirtualColumnPR(mdt);
			
			//fill the values in all above columns in the table
			MyLogger.log("SaveReportStats: savePRMaster: for loop: Start");
			for(PrMaster irMaster : prMasterSet) {
				totalDataToSave++;
				mdt.addRow();
				int rowNo = mdt.getRowCount();
				mdt.updateData(rowNo, "Institution_ID", irMaster.getInstitutionID());
				mdt.updateData(rowNo, "Institution_Name", irMaster.getInstitutionName());
				mdt.updateData(rowNo, "Platform", irMaster.getPlatform());
				mdt.updateData(rowNo, "Data_Type", irMaster.getDataType());
				mdt.updateData(rowNo, "YOP", irMaster.getYop());
				mdt.updateData(rowNo, "Access_Type", irMaster.getAccessType());
				mdt.updateData(rowNo, "Metric_Type", irMaster.getMetricType());
				mdt.updateData(rowNo, "Reporting_Period_Total", Integer.toString(irMaster.getReportingPeriodTotal()));
				mdt.updateData(rowNo, "M_ERROR", "0");
				mdt.updateData(rowNo, getDynamicColumn(), Integer.toString(irMaster.getMetricCount()));
			}
			MyLogger.log("SaveReportStats: savePRMaster: for loop: End");
			
			MyLogger.log("SaveReportStats: savePRMaster: commit mdt Start: Total data to save: " + totalDataToSave + ", table: " + mysqlTableName + " , month: " + getDynamicColumn());
			totalDataToSave = 0;
			//save stats to mysql db
			MyLogger.log("SaveReportStats: savePRMaster: commit mdt End, mdt.commit(con) result : " + mdt.commit(con));
			mdt.destroy();
			prMasterSet.clear();
			
			status = true;
		} catch(Exception e) {
			MyLogger.error("SaveReportStats: savePRMaster: EXCEPTION: " + e);
			throw e;
		} finally {
			if(con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					MyLogger.error("SaveReportStats: savePRMaster: problem while closing db connection: EXCEPTION: " + e);
				}
			}
		}
		return status;
	}

	private static void addVirtualColumnTR(MyDataTable mdt)throws Exception {
		mdt.addColumn("Institution_ID", "-");
		mdt.addColumn("Institution_Name", "-");
		mdt.addColumn("Journal_ID", "-");
		mdt.addColumn("Title", "-");
		mdt.addColumn("Publisher", "-");
		mdt.addColumn("Publisher_ID", "-");
		mdt.addColumn("Platform", "-");
		mdt.addColumn("DOI", "-");
		mdt.addColumn("Proprietary_ID", "-");
		mdt.addColumn("ISBN", "-");
		mdt.addColumn("Print_ISSN", "-");
		mdt.addColumn("Online_ISSN", "-");
		mdt.addColumn("URI", "-");
		mdt.addColumn("Data_Type", "Other");
		mdt.addColumn("Section_Type", "-");
		mdt.addColumn("YOP", "-");
		mdt.addColumn("Access_Type", "Controlled");
		mdt.addColumn("Access_Method", "Regular");
		mdt.addColumn("Metric_Type", "Search_Automated");
		mdt.addColumn("Reporting Period Total", "0");
		mdt.addColumn("M_ERROR", "0");
		mdt.addColumn(getDynamicColumn(), "0");
	}
	
	private static void addVirtualColumnIR(MyDataTable mdt)throws Exception {
		mdt.addColumn("Institution_ID", "-");
		mdt.addColumn("Institution_Name", "-");
		mdt.addColumn("Journal_ID", "-");
		mdt.addColumn("Article_ID", "-");
		mdt.addColumn("Item", "-");
		mdt.addColumn("Publisher", "-");
		mdt.addColumn("Publisher_ID", "-");
		mdt.addColumn("Platform", "-");
		mdt.addColumn("Authors", "-");
		mdt.addColumn("Publication_Date", "-");
		mdt.addColumn("Article_Version", "-");
		mdt.addColumn("DOI", "-");
		mdt.addColumn("Proprietary_ID", "-");
		mdt.addColumn("ISBN", "Other");
		mdt.addColumn("Print_ISSN", "-");
		mdt.addColumn("Online_ISSN", "-");
		mdt.addColumn("URI", "-");
		mdt.addColumn("Parent_Title", "-");
		mdt.addColumn("Parent_Authors", "-");
		mdt.addColumn("Parent_Publication_Date", "-");
		mdt.addColumn("Parent_Article_Version", "-");
		mdt.addColumn("Parent_Data_Type", "-");
		mdt.addColumn("Parent_DOI", "-");
		mdt.addColumn("Parent_Proprietary_ID", "-");
		mdt.addColumn("Parent_ISBN", "-");
		mdt.addColumn("Parent_Print_ISSN", "-");
		mdt.addColumn("Parent_Online_ISSN", "-");
		mdt.addColumn("Parent_URI", "-");
		mdt.addColumn("Component_Title", "-");
		mdt.addColumn("Component_Authors", "-");
		mdt.addColumn("Component_Article_Version", "-");
		mdt.addColumn("Component_Data_Type", "-");
		mdt.addColumn("Component_DOI", "-");
		mdt.addColumn("Component_Proprietary_ID", "-");
		mdt.addColumn("Component_ISBN", "-");
		mdt.addColumn("Component_Print_ISSN", "-");
		mdt.addColumn("Component_Online_ISSN", "-");
		mdt.addColumn("Component_URI", "-");
		mdt.addColumn("Data_Type", "-");
		mdt.addColumn("Section_Type", "-");
		mdt.addColumn("YOP", "0");
		mdt.addColumn("Access_Type", "Controlled");
		mdt.addColumn("Access_Method", "Regular");
		mdt.addColumn("Metric_Type", "Search_Automated");
		mdt.addColumn("Reporting Period Total", "0");
		mdt.addColumn("M_ERROR", "0");
		mdt.addColumn(getDynamicColumn(), "0");
	}
	
	private static void addVirtualColumnPR(MyDataTable mdt)throws Exception {
		mdt.addColumn("Institution_ID", "-");
		mdt.addColumn("Institution_Name", "-");
		mdt.addColumn("Platform", "-");
		mdt.addColumn("Data_Type", "-");
		mdt.addColumn("YOP", "0");
		mdt.addColumn("Access_Type", "Controlled");
		mdt.addColumn("Metric_Type", "Other");
		mdt.addColumn("Reporting_Period_Total", "-");
		mdt.addColumn("M_ERROR", "0");
		mdt.addColumn(getDynamicColumn(), "0");
	}

	public static String getDynamicColumn() {
		return dynamicColumn;
	}

	public static void setDynamicColumn(String dynamicColumn) {
		SaveReportStats.dynamicColumn = dynamicColumn;
	}
}
