package org.vincentg.mqi.record;

import java.io.Serializable;

import org.vincentg.mqi.enums.EnumRecType;

public class RecordProcessorFactory implements Serializable {

	private static final long serialVersionUID = 2862666057395630881L;

	public RecordProcessor getRecordProcessor(EnumRecType recType) {

		RecordProcessor recProc = null;

		if ( recType == EnumRecType.JSON ) {
			recProc = new JSONRecProcessor();
		} else {
			recProc = new DelimitedRecProcessor();
		}

		return recProc;
	}

}
