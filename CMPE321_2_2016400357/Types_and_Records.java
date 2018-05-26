package dbmsproject2;

/*
 * YUSUF KALAYCI
 * 2016400357
 * */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

public class Types_and_Records {
	String typeName;
	String primary_key;
	String[] fieldNames;
	String[] fields;
	long location;
	long FirstRecordLoc;
	long LastRecordLoc;
	int[] fieldLengths;
	boolean foundRecord;
	int numberOfFields;
    int pageToPageNmbrOfRec;
    int sizeOfRecord;
    int numberOfRecords;
    int numberOfPage;
   
    InputStreamReader myInput = new InputStreamReader(System.in);
    BufferedReader in = new BufferedReader(myInput);
    
     
    //list all records and calls from the sysCatalogue
    public void ListRecords(String typeName)  {
    	try {

    		File nfile = new File("myDataFile/" + typeName + ".dat");
    		RandomAccessFile ras = new RandomAccessFile(nfile, "r");
    		
    		
    		long compute = ras.getFilePointer();
			ras.seek(160);
			readTypeHeader(typeName);
			
			if(LastRecordLoc == 0 || LastRecordLoc == compute - sizeOfRecord) {
				System.out.println("THERE IS NO RECORD IN THIS TYPE\n\n");
				return;
			}
			else {
		
			System.out.println("\n");
			while(compute <= LastRecordLoc) {
				long pageNum = (compute - 160) / sizeOfRecord / pageToPageNmbrOfRec + 1;
				System.out.print("PAGE #" + pageNum + ": ");
				System.out.print((compute - 160) / sizeOfRecord + 1 +". RECORD: ");
				for(int i = 0; i < numberOfFields; i++) {
					System.out.print(fieldNames[i] + ": ");
					for(int j = 0; j < fieldLengths[i]; j++)
						System.out.print(ras.readChar());
					if(i != numberOfFields - 1)
					   System.out.print(", ");
				}
				System.out.println();
				compute = ras.getFilePointer();
			}
			System.out.println("\n######################################################################\n");
			
    		
			}
    		
    	} catch(IOException e) {
  		   e.printStackTrace();
  	   }
    	
    	
    }
    
  
  //create record with calls from the sysCatalogue
    public void createRecord(String typeName) {
        this.typeName = typeName;
		System.out.println("\nFIELDS:");
		try {
			File file1 = new File("myDataFile/" + typeName + ".dat");
			RandomAccessFile ras = new RandomAccessFile(file1, "rw");
			
			readTypeHeader(typeName);
			
			fields = new String[numberOfFields];
            
			
			File nfile = new File(typeName + "_idx.dat");
			RandomAccessFile ras1 = new RandomAccessFile(nfile, "rw");
			ras1.seek(0);
			numberOfRecords = ras1.readInt();
			int recordSize = fieldLengths[0] * 2 + 8;		
			System.out.println("\nWRITE PRIMARY KEY  (" + fieldNames[0] + "): ");
			location = 8;
			fields[0] = in.readLine();
			Search(0, numberOfRecords - 1, fields[0], recordSize);
			boolean b = fields[0].length() != fieldLengths[0];
			
			while(foundRecord || b) {
				if(foundRecord)
				    System.out.println("\nTHIS PRIMARY KEY ALREADY USED,WRITE ANOTHER :\n");
				else
					System.out.println("\nPRIMARY KEY LENGTH SHOULD " + fieldLengths[0] + ".\nWRITE ANOTHER FIELD " + fieldLengths[0] + ":");
				fields[0] = in.readLine();
				b = fields[0].length() != fieldLengths[0];
			    Search(0, numberOfRecords - 1, fields[0], recordSize);			   
			}
	
			for(long i =  (numberOfRecords - 1) * (recordSize) + 8; i >= location; i = i - recordSize) {
				   ras1.seek(i);
				   Byte[] byt = new Byte[recordSize];
				   for(int y = 0; y < recordSize; y++) {
					   byt[y] = ras1.readByte();
				   }
				   for(int y = 0; y < recordSize; y++) {
					   ras1.writeByte(byt[y]);
				   }
			   }	
			
			//writes primary key to the index file
			ras1.seek(location);
			for(int i = 0; i < fields[0].length(); i++)
				ras1.writeChar(fields[0].charAt(i));
			
			//change the last record location address
			if(LastRecordLoc == 0) {
				LastRecordLoc = 160;
			    ras.seek(160);
			}
			else {
			ras.seek(LastRecordLoc + sizeOfRecord);
			LastRecordLoc += sizeOfRecord;
			}
		//	lastRecLocation(ras, ras1);
			
			for(int x = 0; x < fieldLengths[0]; x++) {
				ras.writeChar(fields[0].charAt(x));
			}
					
	    	for(int x = 1; x < numberOfFields; x++) {
		 	    System.out.print("\n" + fieldNames[x] + ": ");
		 		String field = in.readLine();
		
		 		while(field.length() != fieldLengths[x]) {
		 			System.out.println("\nFIELD LENGTH SHOULD " + fieldLengths[x] + ".\nWRITE ANOTHER" + fieldLengths[x] + ":\n");
		 			field = in.readLine();
		 		   }
		 		   fields[x] = field;
		 		   for(int y = 0; y < fieldLengths[x]; y++)
		 			   ras.writeChar(fields[x].charAt(y));		 		  
		 	   }
	    	
	    	//increments number of records
	    	ras.seek(8);
	    	ras.writeLong(LastRecordLoc);
	    	ras1.writeLong(LastRecordLoc);
	    	ras1.seek(0);
	    	numberOfRecords++;
	    	ras1.writeInt(numberOfRecords);
	    	
	    	int pageID = (numberOfRecords - 1) / pageToPageNmbrOfRec + 1;
	    	
	    	System.out.println("\nTHE RECORD INSERTED THE PAGE #" + pageID + " OF TABLE " + typeName 
	    			+ "!!!\n############################################################################3\n");
	    	UpdateSystemCatalog();
	    	ras.close();
	    	ras1.close();
	    	
	    	
		} catch (IOException ex) {
		     ex.printStackTrace();
	    }
		
    }
    
private void lastRecLocation(RandomAccessFile ras, RandomAccessFile ras1) {
    	// TODO Auto-generated method stub
        	
        	if(LastRecordLoc == 0) {
    			LastRecordLoc = 160;
    		    try {
    				ras.seek(160);
    			} catch (IOException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    		}
    		else {
    		try {
    			ras.seek(LastRecordLoc + sizeOfRecord);
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    		LastRecordLoc += sizeOfRecord;
    		}
    	
    }



  
    //create record with calls from the sysCatalogue
    public void DeleteRecord(String typeName) {
    	this.typeName = typeName;
    	readTypeHeader(typeName);
    	if(LastRecordLoc == 0) {
			System.out.println("THERE IS NO RECORD IN THE TYPE!!!!!\n...\n\n");
			return;
		}
    	
    	try {
    	   File nfile = new File(typeName + "_idx.txt");
    	   File nfile1 = new File("myDataFile/" + typeName + ".dat");
    	   RandomAccessFile index = new RandomAccessFile(nfile, "rw");
    	   RandomAccessFile data = new RandomAccessFile(nfile1, "rw");
    	   
    	   index.seek(0);
    	   numberOfRecords = index.readInt();
    	   int recordSize = index.readInt();
    	   
    	   //reads primary key and searches it in the index file
    	   System.out.println("\nWRITE PRIMARY KEY FOR DELETION :\n");
    	   String primaryKey = in.readLine();
    	   
    	   Search(0, numberOfRecords - 1, primaryKey, recordSize);
    	   
    	   while(!foundRecord) {
    		   System.out.println("\nTHE PRIMARY KEY NOT FOUND WRITE ANOTHER :\n");
    		   System.out.println("(TO SEE LIST PRESS (1)!)\n");
   			primaryKey = in.readLine();
   			if(primaryKey.equals("1")) {
   				ListRecords(typeName);
   				DeleteRecord(typeName);
   				return;
   			}
    		   Search(0, numberOfRecords - 1, primaryKey, recordSize);
    	   }
    	   
    	   index.seek(location + fieldLengths[0]*2);
    	   long address = index.readLong();
    	   
    	   //prints the information of the record
    	   data.seek(address);
   		   System.out.println("\nTHE RECORD WANT TO DELETE PAGE #" + ((address - 160) / sizeOfRecord / pageToPageNmbrOfRec + 1));
   		   System.out.println("FIELDS OF THE RECORD : ");
   		   for(int i = 0; i < numberOfFields; i++) {
   			System.out.print(fieldNames[i] + ": ");
   			for(int j = 0; j < fieldLengths[i]; j++) 
   				System.out.print(data.readChar());
   			System.out.print(", ");
   		}
   		System.out.println("\n");
    	  
   	       //records which are location after the deleted record moves by 1 record size
    	   for(long i = location + recordSize; i <= ((numberOfRecords - 1) * recordSize + 8); i += recordSize) {
    		  index.seek(i);
    		  Byte[] b = new Byte[recordSize];
    		  for(int j = 0; j < recordSize; j++)
    			   b[j] = index.readByte();
    	      index.seek(i - recordSize);
    	      for(int j = 0; j < recordSize; j++)
    	    	  index.writeByte(b[j]);
    	   }
    	   
    	   //decrements number of records and updates last record location and system catalog file
    	   numberOfRecords--;
    	   index.seek(0);
    	   index.writeInt(numberOfRecords);
    	   
    	   data.seek(LastRecordLoc);
    	   String str = "";
    	   for(int i = 0; i < fieldLengths[0]; i++)
    		   str += data.readChar();
    	   
    	   data.seek(LastRecordLoc);
    	   Byte[] b = new Byte[sizeOfRecord];
    	   for(int i = 0; i < sizeOfRecord; i++)
    		   b[i] = data.readByte();
    	   
    	   data.seek(address);
    	   for(int i = 0; i < sizeOfRecord; i++)
    		   data.write(b[i]);
    	   
    	   LastRecordLoc -= sizeOfRecord;
    	   data.seek(8);
    	   data.writeLong(LastRecordLoc);
    	   
    	   Search(0, numberOfRecords - 1, str, recordSize);
    	   
    	   index.seek(location + 2 * fieldLengths[0]);
    	   index.writeLong(address);
    	   
    	   UpdateSystemCatalog();
    	  
    	   System.out.println("\nTHE RECORD IS DELETED!!!\n#################################################################\n");
    	   index.close();
    	   data.close();
    	   
    	} catch(IOException e) {
    		   e.printStackTrace();
    	   }
    }
    //search record with the help of primary key
    public void SearchRecord(String typeName) {
    	this.typeName = typeName;
    	try {
     	   File nfile = new File(typeName + "_idx.dat");
     	   File nfile1 = new File("myDataFile/" + typeName + ".dat");
     	   RandomAccessFile index = new RandomAccessFile(nfile, "rw");
     	   RandomAccessFile data = new RandomAccessFile(nfile1, "rw");
     	   
     	   readTypeHeader(typeName);
     	   if(LastRecordLoc == 0) {
				System.out.println("\nTHERE IS NO RECORD IN THE TYPE \n...\n\n");
				return;
			}
     	   
     	   index.seek(0);
     	   numberOfRecords = index.readInt();
     	   int recordSize = index.readInt();
     	   
     	   System.out.println("\nWRITE PRIMARY KEY YOU WANT TO SEARCH:\n");
     	   
     	   //reads primary key
     	   String primaryKey = in.readLine();
     	   
     	   System.out.println("\nPRESS (1) TO SEE THE RECORD WHİCH EQUAL YOUR'S WRITTEN!!\n");
     	  
     	   int choice = Integer.parseInt(in.readLine());
     	   
     	   Search(0, numberOfRecords - 1, primaryKey, recordSize);
     	   
     	   System.out.println();
     	 
     	   if(choice == 1) {
     	     if(!foundRecord) {
     		   System.out.println("\nTHE PRIMARY KEY IS NOT FOUND!!!");
     		   return;
     	     }
     		   
     	     index.seek(location + 2 * fieldLengths[0]);
     	     long address = index.readLong();
     	     data.seek(address);
     	     System.out.print("\nPage #" + ((address - 160) / sizeOfRecord / pageToPageNmbrOfRec + 1) + ": ");
     	     for(int i = 0; i < numberOfFields; i++) {
				   System.out.print(fieldNames[i] + ": ");
				   for(int j = 0; j < fieldLengths[i]; j++)
					   System.out.print(data.readChar());
				   if(i != numberOfFields - 1)
				      System.out.print(", ");
			   }
			   System.out.println("\n");
     	     
     	   } 
     	  System.out.println("\n#####################################################################\n");
     	   
     	   
    	 } catch(IOException e) {
  		   e.printStackTrace();
  	   }
    	catch (NumberFormatException num) {
     	   System.out.println("\nERROR!! THE CHARACTER İS NOT ALLOWED.\n...\n");
        }
    }
   
    //updates system catalog file with new information
    public void UpdateSystemCatalog() {
    	try {
    	File files = new File("SystemCatalog.dat");
    	RandomAccessFile rasf = new RandomAccessFile(files, "rw");
    	rasf.seek(0);
    	int NumOfTables = rasf.readInt();
    	
    	for(long x = 4; x <= (NumOfTables-1) * 162 + 4; x += 162) {
    		rasf.seek(x);
    		int length = 0;
    		for(int y = 0; y < typeName.length(); y++) {       			
    			if(typeName.charAt(y) == rasf.readChar()) 
    				length++;    		
    		}
    		if(length == typeName.length() && (rasf.readChar() == '*' || length == 10))
    			break;
    	}
    	
    	long loc = rasf.getFilePointer();
    	rasf.seek(loc - 2 - 2*typeName.length() + 15);
    	int pageID = (numberOfRecords - 1)/ pageToPageNmbrOfRec + 1;
    	rasf.writeInt(pageID);
    	rasf.writeInt(numberOfRecords);
    
    } catch(IOException e) {
		   e.printStackTrace();
	   }
    } 
    //create type method with calls from the sysCatalogue
    public boolean CreateType(String typeName) {
        try {   
      	  
      	  System.out.println("\nNUMBER OF FIELDS IN EACH RECORDS :\n");
	          
            numberOfFields = Integer.parseInt(in.readLine());
	      
	          while(numberOfFields > 10) {
		        System.out.println("\nWRITE LESS THEN 10 FIELD NUMBERS :\n");
		        numberOfFields = Integer.parseInt(in.readLine());
	   }
	          
	          //reads lengths of fields
	          sizeOfRecord = 0;
	          fieldLengths = new int[numberOfFields];
	          System.out.println("\nLENGTHS OF THE FIELDS :\n");
	          for(int x = 1; x <= numberOfFields; x++) {
	  		     System.out.print(x + ". field length: ");
	  		     int length = Integer.parseInt(in.readLine());
	  		     while(length > 10) {
	  			   System.out.print("\nWRITE LESS THEN 10 FIELD LENGTH\n\n" + x + ". FIELD LENGTH: ");
	  			   length = Integer.parseInt(in.readLine());
	  		   }
	  		   fieldLengths[x - 1] = length;
	  		   sizeOfRecord += length;
	  	   }
	          sizeOfRecord = sizeOfRecord * 2;
	      	   
	       // reads names of fields   
	       fieldNames = new String[numberOfFields];
	       
	       System.out.println("\nField names:\n");
	       for(int x = 1; x <= numberOfFields; x++) {
		   System.out.print(x + ". FIELD NAMES: ");
		   String fieldName = in.readLine();
		   while(fieldName.length() > 10) {
			   System.out.print("\nWRITE LESS THEN 10 CHAR FOR FIELD NAME \n\n" + x + ". FIELD NAME : ");
			   fieldName = in.readLine();
		   }
		   fieldNames[x - 1] = fieldName;
	   }
	       primary_key = fieldNames[0]; 	 
	       
	       headerToDAta();
	        
		       try {
		    	   
		    	  //writes header information to the data file
		    	  File nfile = new File("myDataFile/" + typeName + ".dat");
		    	  
		    	  RandomAccessFile raf = new RandomAccessFile(nfile, "rw");
		    	 
		    	  pageToPageNmbrOfRec = 1024 / sizeOfRecord;
		    	  raf.writeInt(pageToPageNmbrOfRec);
		    	  raf.writeInt(sizeOfRecord);
		    	  raf.writeLong(0);
		    	  raf.writeInt(numberOfFields);
		    	  for(int x = 0; x < numberOfFields; x++) 
		    		 raf.writeInt(fieldLengths[x]);
		    	 for(int x = 0; x < 10 - numberOfFields; x++) 
		    		 raf.writeInt(0);
		  
		    	  for(int x = 0; x < numberOfFields; x++) {
		    		  for(int y = 0; y < fieldNames[x].length(); y++) 
		    		      raf.writeChar(fieldNames[x].charAt(y));
		    		  
		    		 for(int y = 0; y < 10 - fieldNames[x].length(); y++) 
		    		      raf.writeChar('*'); 
		    	  }
		    	  
		    	 for(int x = 0; x < (10 - numberOfFields) * 10; x++) 
		    		   raf.writeChar('*');
		         
		    	 //writes header information to the index file
		    	 File nfile2 = new File (typeName + "_idx.dat");
		    	 RandomAccessFile raf1 = new RandomAccessFile(nfile2, "rw"); 
		    	 raf1.writeInt(0);
		    	 raf1.writeInt(2 * fieldLengths[0] + 8);
		    	 raf.close();
		    	 raf1.close();
		       }
		  catch (FileNotFoundException ex) {
	          ex.printStackTrace();
	      } catch (IOException ex) {
	          ex.printStackTrace();
	} 
	  
        }   catch (IOException ex) {
   ex.printStackTrace();
  }
     catch (NumberFormatException num) {
  	   System.out.println("\nERROR!! THE CHARACTER İS NOT ALLOWED.\n...\n");
  	   return false;
     }
        return true;

 
}
    //use in the create type method 
    public void headerToDAta() {
    	
        

 }
    //search the file and calls inside the methods
   public void Search(long min, long max, String PrimaryKey, int recordSize) {
  	   try {
  		   File f = new File(typeName + "_idx.dat");
  		   RandomAccessFile index = new RandomAccessFile(f, "r");
             if(max < min) {
          	   foundRecord = false;
             }
             else      {     
               long mid = (min + max) / 2;
               long midd= mid * recordSize + 8;
               index.seek(midd);
               
               String str = "";
               for(int i = 0; i < (recordSize - 8)/ 2; i++) 
             	  str += index.readChar();
     
               if (str.compareTo(PrimaryKey) > 0) {
              	location = midd;
                 Search(min, mid-1, PrimaryKey, recordSize);
               }
               else if (str.compareTo(PrimaryKey) < 0) {
              	location = midd + recordSize;
              	Search(mid+1, max, PrimaryKey, recordSize);
               }
               else {
              	foundRecord = true;
                 location = midd;
               }
             }		   
             index.close();
  	   } catch(IOException e) {
  		   e.printStackTrace();
  	   }
     }

   //reads the information on the table header
    public void readTypeHeader(String typeName) {
    	try {
    		
    		File nfile1 = new File("myDataFile/" + typeName + ".dat");
    		RandomAccessFile raf = new RandomAccessFile(nfile1, "r");
    		
    		raf.seek(0);
    		pageToPageNmbrOfRec = raf.readInt();
			sizeOfRecord = raf.readInt();
			LastRecordLoc = raf.readLong();
			numberOfFields = raf.readInt();
			fieldLengths = new int[numberOfFields];
			fieldNames = new String[numberOfFields];
	
			for(int x = 0; x < numberOfFields; x++)
				fieldLengths[x] = raf.readInt();
			
			raf.seek(60);
			for(int x = 0; x < numberOfFields; x++) {
				fieldNames[x] = "";
				for(int y = 0; y < 10; y++) {
					char chr = raf.readChar();
					if(chr != '*')
						fieldNames[x] += chr;
				}
			}
			raf.close();
    	} catch(IOException e) {
   		   e.printStackTrace();
   	   }
    }    

}
