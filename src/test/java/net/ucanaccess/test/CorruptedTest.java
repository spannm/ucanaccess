package net.ucanaccess.test;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class CorruptedTest extends UcanaccessTestBase{
	
	public CorruptedTest() {
		super();
	}
	
	public CorruptedTest(FileFormat accVer) {
		super(accVer);
	}
	
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/corrupted.accdb";
	}
	public void testCorrupted(){
		System.out.println(super.ucanaccess);
	}
}
