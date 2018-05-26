package dbmsproject2;

/*
 * YUSUF KALAYCI
 * 2016400357
 * */


import java.io.*;




public class sysCatalogue {
	
	
	InputStreamReader myInput = new InputStreamReader(System.in);
    BufferedReader bis = new BufferedReader(myInput);
    
  //main method for Dbms
  public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			sysCatalogue mySysCatalogue = new sysCatalogue();
			mySysCatalogue.operationsMenu();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
    
    
  //sysCatalogue class constructor  
  public sysCatalogue() throws IOException{
	  
	//create a sysCatalogue dat file
    	File file = new File("sysCatalogue.dat");  
    	boolean ok = (new File("myDataFile")).mkdir();
    	   
    	  try {
    		  RandomAccessFile rasf = new RandomAccessFile(file, "rw");
    		  rasf.seek(0);
    		  if(ok)  
    		     rasf.writeInt(0);   
    	  } catch (IOException e) {
  			e.printStackTrace();
    	  }
 
    	
    }  
 
  
  public void operationsMenu() {
  	try {
  		//Whole operations which Dbms does
  		
	  System.out.println("WELCOME YUSUF'S DBMS! PLEASE SELECT YOUR OPERATION!!!!!!!!!!\n");
	  System.out.println("(DDL)Type Operations:");
      System.out.println("(1) List all types");
      System.out.println("(2) Create a type");
      System.out.println("(3) Delete a type");
      System.out.println("\n(DML)Record Operations:");
      System.out.println("(4) List all records of a type");
      System.out.println("(5) Create a record");
      System.out.println("(6) Delete a record");
      System.out.println("(7) Search for a record(by primary key)");
     
      System.out.println("\n(8) EXIT YUSUF'S DBMS");
      System.out.println();
      
      int selection = Integer.parseInt(bis.readLine());
      System.out.println();
      


      switch(selection)
		{
			case 1:
				listAllTypes();
		      	operationsMenu();
				break;
			case 2:
				createType();
		      	operationsMenu();
		      	break;
			case 3:
				deleteType();
		      	operationsMenu();
				break;
			case 4:
				listAllRecord();
		      	operationsMenu();
				break;
			case 5:
				createRecord();	
		      	operationsMenu();
				break;
			case 6:
				deleteRecord();
		      	operationsMenu();
				break;
			case 7:
				searchForRecord();
		      	operationsMenu();
				break;
			case 8:
				System.out.println("EXIT THE SYSTEM!!!");
		      	break;
			default:
				 operationsMenu();
				break;	
		}
      
   
  } catch (IOException e) {
		e.printStackTrace();
	}
  }
    
//listAllTypes on the terminal with the creation order
  public boolean listAllTypes() {
  	try {
  	  RandomAccessFile out = new RandomAccessFile("sysCatalogue.dat", "rw");
        out.seek(0);
        
        int numberOfTypes = out.readInt();
 
        switch(numberOfTypes) {
    		case 1:
    			System.out.println("\nDATABASE HAS NO TYPE PLEASE CREATE FIRST!!\n...");
			    break;
    		case 2:
    			System.out.println("\nTHERE IS  " + numberOfTypes + " TYPES!\n");
	      	    break;
            default:
            	System.out.println("\nTHERE ARE  " + numberOfTypes + " TYPES!\n");
            	break;
            	
        }
        for(int x = 1; x <= numberOfTypes; x++) {
      	  out.seek((x - 1) * 162 + 4);
      	  System.out.print(x + ". TYPE NAME : ");
      	 
      	  for(int y = 0; y < 15; y++) {
      		  char charac = out.readChar();
      		  if(charac != '*')
      			  System.out.print(charac);
       		 
       	  }
      	  
      	  System.out.print("\n PAGE NUMBER: " + out.readInt() + ",\n RECORD NUMBER: " + out.readInt());
      	  int numOfFields = out.readInt();
      	  System.out.println(",\n FIELD NUMBER: " + numOfFields);
      	  System.out.println("\nFIELDS NAMES :");
      	  for(int y = 0; y < numOfFields; y++) {
      		  for(int z = 0; z < 10; z++) {
      			  char charac = out.readChar();
      			  if(charac != '*')
      			     System.out.print(charac);
      		  }
      		  if(y != numOfFields - 1) 
      		     System.out.print(",\n ");
      	  }
      	  System.out.println("\n###########################################################\n");
      	  
        }
        
        out.close();
  	} catch (IOException ex) {
  		
          ex.printStackTrace();    	
  }
  	return true;
  }
  
  
//Creates a type with using the operations menu 
  public void createType() throws IOException
  {
  	
		int choice=0;
		
		// take the table name to create
		System.out.println(" ENTER THE NAME OF THE TYPE :");
		String typeName = bis.readLine();
		while(typeExist(typeName))
		{
			System.out.println("ERROR: THE TYPE ALREADY EXISTS.");
			System.out.println();
			System.out.println("(0) RETURN TO OPERATION MENU...");
			System.out.println("(1) ENTER ANOTHER TYPE NAME...");
			
		 try
			{
				choice = Integer.parseInt(bis.readLine()); 
				if(choice==1)
				{
					System.out.println("ENTER THE NAME OF TYPE");
					typeName = bis.readLine();
				}
				else
					return;
			} 
			catch (NumberFormatException e) 
			{
				return;	
			}
			
		}
		
		//createTypes_and_Records();
		 
		Types_and_Records type = new  Types_and_Records();
 	   boolean b = type.CreateType(typeName);
 	   if(b == false)
 		   return;
	
		
 	   
 	   RandomAccessFile raf = new RandomAccessFile("sysCatalogue.dat", "rw");
 	   raf.seek(0);
 	   int numberOfTypes = raf.readInt();
 	   numberOfTypes++;
 	   raf.seek(0);
 	   raf.writeInt(numberOfTypes);
 	   
 	   
 	   raf.seek((numberOfTypes - 1) * 162 + 4);    //Adds the information to System Catalog file about new type
 	   
 	   for(int x = 0; x < typeName.length(); x++) 
 		   raf.writeChar(typeName.charAt(x));
 	 
 	   for(int x = 0; x < 15 - typeName.length(); x++)
 		   raf.writeChar('*');
 	   
 	   raf.writeInt(0);
 	   raf.writeInt(0);
 	   raf.writeInt(type.numberOfFields);
 	   
 	   for(int x = 0; x < type.numberOfFields; x++) {
 	      for(int y = 0; y < type.fieldNames[x].length(); y++) 
 		   raf.writeChar(type.fieldNames[x].charAt(y));
 	 
 	      for(int y = 0; y < 10 - type.fieldNames[x].length(); y++)
 		   raf.writeChar('*');
 	   }
 	   for(int x = 0; x < (10 - type.numberOfFields) * 10; x++) 
 		   raf.writeChar('*');
 	   
 	 raf.close();
	System.out.println("\n#######################TABLE IS CREATED!!!########################"
			+ "\n#############################################################################################\n"); 
 	
}
 // use if you want to create double table at the same time  
  public void createTypes_and_Records() {
	  String typeName = null;
	  Types_and_Records type = new  Types_and_Records();
	   boolean b = type.CreateType(typeName);
	   if(b == false)
		   return;
	   
  }
  
  // Deletes type with using the operations menu 
  public int deleteType(){
		System.out.println("ENTER THE TYPE NAME : ");
		try {
				String typeName = bis.readLine();
				if(typeExist(typeName)){
					File typeFile = new File("./myDataFile/"+typeName+".dat");
					File indexFile = new File("./nameOfType/"+typeName+".idx");
					typeFile.delete();
					indexFile.delete();
					System.out.println("################### TYPE "+typeName+" DELETED SUCCESFULLY!!!!!!!#########################"
							+ "\n#############################################################################################\n");
					return 0;
				}
			
			
				else
				{
					System.out.println("THE TYPE NAME DOES NOT EXIST!");
					return 0;
				}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
  
//lists all records in a selected type 
  public void listAllRecord() {
	  	System.out.println("\nENTER TYPE NAME :\n");
	  	try {
	  		//reads all the type names from the sysCatalogue
	  		String nameOfType = bis.readLine();
	  		while(!typeExist(nameOfType)) {
	  			System.out.println("\nTHERE IS NO TYPE WITH THIS NAME PRESS (1) TO SEE TYPE NAMES !!!!!!:\n");
	          	nameOfType = bis.readLine();
	          	if(nameOfType.equals("1")) {
	          		boolean b = listAllTypes();
	          		if(b)
	          		  listAllRecord();
	          		return;
	          	}
	  		}
	  	
	  		Types_and_Records type = new Types_and_Records();
	  		type.ListRecords(nameOfType);
	  		
	  		
	  	} catch (IOException ex) {
	          ex.printStackTrace();
	}
	  	
	  }

  
  //create a record in selected type
  public void createRecord() {
  	System.out.println("\n ENTER THE TYPE NAME :\n");
  	try {
  		
  		//reads all the types name
  		String nameOfType = bis.readLine();
  		while(!typeExist(nameOfType)) {
  			System.out.println("\nTHERE IS NO TYPE WITH THIS NAME PRESS (1) TO SEE TYPE NAMES !!!!!! :\n");
          	nameOfType = bis.readLine();
          	if(nameOfType.equals("1")) {
          		boolean b = listAllTypes();
          		if(b)
          		   createRecord();
          		return;
          	}
          	boolean bool = listAllTypes();
      		if(bool)
      		   createRecord();
  		}
  		
  		Types_and_Records type = new Types_and_Records();
  		type.createRecord(nameOfType);
  		
  		
  		
  		
  	} catch (IOException ex) {
          ex.printStackTrace();
}
  }
  
  
 
  //deletes a record in selected type
  public void deleteRecord() {
  	System.out.println("\nENTER TYPE NAME :\n");
  	try {
  		//reads all the type names
  		String nameOfType = bis.readLine();
  		while(!typeExist(nameOfType)) {
  			System.out.println("\nTHERE IS NO TYPE WITH THIS NAME PRESS (1) TO SEE TYPE NAMES !!!!!!:\n");
          	nameOfType = bis.readLine();
          	if(nameOfType.equals("1")) {
          		boolean b = listAllTypes();
          		if(b)
          		   deleteRecord();
          		return;
          	}
  		}
  		
  		Types_and_Records type = new Types_and_Records();
  		type.DeleteRecord(nameOfType);
  		
  		
  	} catch (IOException ex) {
          ex.printStackTrace();
}
  }
  
  //searches a record in a selected type
  public void searchForRecord() {
  	System.out.println("\nENTER TYPE NAME :\n");
  	try {
  		String nameOfType = bis.readLine();
  		while(!typeExist(nameOfType)) {
  			System.out.println("\nTHERE IS NO TYPE WITH THIS NAME PRESS (1) TO SEE TYPE NAMES !!!!!!:\n");
  			nameOfType = bis.readLine();
          	if(nameOfType.equals("1")) {
          		boolean b = listAllTypes();
          		if(b)
          		   searchForRecord();
          		return;
          	}
  		}
  	
  		Types_and_Records type = new Types_and_Records();
  		type.SearchRecord(nameOfType);
  		
  		
  	} catch (IOException ex) {
          ex.printStackTrace();
}
  }
 
  //Check the type among the whole directory and type name exists or not.
  public boolean typeExist(String nameOfType) {
  	File dir = new File("myDataFile");
  	String[] f = dir.list();
  	for(int x = 0; x < f.length; x++) {
  		if (f[x].compareTo(nameOfType + ".dat")==0) 
  			return true;		
  	}
  	return false;
  }
  



}

	
	
	
	

































