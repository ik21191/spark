package com.mps.stats.comparator;

import java.io.Serializable;
import java.util.Comparator;
import org.bson.Document;
import com.mps.utils.Constants;
import com.mps.utils.MyLogger;

public class TRMasterSortDocument implements Comparator<Document>, Serializable {
	
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Document doc1, Document doc2) {
		try {
			String institutionId1 = doc1.get(Constants.INSTITUTION_ID).toString();
			String institutionId2 = doc2.get(Constants.INSTITUTION_ID).toString();
			// compare session
			int institutionIdCompare = institutionId1.compareTo(institutionId2);
			if (institutionIdCompare == 0) {
				// compare user agent
				String journalId1 = doc1.get(Constants.JOURNAL_ID).toString();
				String journalId2 = doc2.get(Constants.JOURNAL_ID).toString();
				int journalIdCompare = journalId1.compareTo(journalId2);
				if (journalIdCompare == 0) {
					String sessionId1 = doc1.get(Constants.SESSION_ID).toString();
					String sessionId2 = doc2.get(Constants.SESSION_ID).toString();
					int sessionCompare = sessionId1.compareTo(sessionId2);
					if(sessionCompare == 0) {
						String articleId1 = doc1.get(Constants.ARTICLE_ID).toString();
						String articleId2 = doc2.get(Constants.ARTICLE_ID).toString();
						return articleId1.compareTo(articleId2);
					} else {
						return sessionCompare;
					}
					
				} else {
					return journalIdCompare;
				}
			} else {
				return institutionIdCompare;
			}
		} catch(Exception e) {
			MyLogger.error("SortDocument: compare: EXCEPTION for Doc1: " + doc1);
			MyLogger.error("SortDocument: compare: EXCEPTION for Doc2: " + doc1);
			MyLogger.error("SortDocument: compare: EXCEPTION: " + e);
			throw e;
		}
		

	}
}