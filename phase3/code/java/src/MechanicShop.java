/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int numOfCustomerRequests = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++numOfCustomerRequests;
		}//end while
		stmt.close ();
		return numOfCustomerRequests;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int numOfCustomerRequests = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int numOfCustomerRequests = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			numOfCustomerRequests++;
		}//end while
		stmt.close ();
		return numOfCustomerRequests;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { 
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}
		}while (true);
		return input;
	}
	
	public static void AddCustomer(MechanicShop esql){
		try{
			String querySize = String.format("SELECT id FROM Customer");
			List<List<String>> data = esql.executeQueryAndReturnResult(querySize);
			int newCustomerID = data.size() + 1;
			System.out.print("Enter new customer's first name: "); String customerFName = in.readLine();
			System.out.print("Enter new customer's last name: "); String customerLName = in.readLine();
			System.out.print("Enter new customer's phone number: "); String customerPhone = in.readLine();
	        System.out.print("Enter new customer's address: "); String customerAddress = in.readLine();

            String query = String.format("INSERT INTO Customer(id, fname, lname, phone, address) VALUES(%d, '%s', '%s', '%s', '%s')", newCustomerID,customerFName,customerLName,customerPhone,customerAddress);
            esql.executeUpdate(query);

      		}catch(Exception e){ System.err.println (e.getMessage()); }
	} 
	
	public static void AddMechanic(MechanicShop esql){
		try{
			String querySize = String.format("SELECT id FROM Mechanic");
			List<List<String>> data = esql.executeQueryAndReturnResult(querySize);
			int newmechID = data.size() + 1;

			System.out.print("Enter new mechanic's first name: "); String mechanicFName = in.readLine();
			System.out.print("Enter new mechanic's last name: "); String mechanicLName = in.readLine();
			System.out.print("Enter new mechanic's years of experience: "); String yearsExperienceString = in.readLine();
            int yearsExperience = Integer.parseInt(yearsExperienceString);

			String query = String.format("INSERT INTO mechanic(id, fname, lname, experience) VALUES(%d, '%s', '%s', %d)", newmechID, mechanicFName, mechanicLName, yearsExperience);
            esql.executeUpdate(query);
              
            }catch(Exception e){ System.err.println (e.getMessage()); }
	}
	
	public static void AddCar(MechanicShop esql){
		try{
			System.out.print("Enter new car's VIN: "); String newCarVin = in.readLine();
			System.out.print("Enter new car's make: "); String carMake = in.readLine();
			System.out.print("Enter new car's model: "); String carModel = in.readLine();
			System.out.print("Enter new car's year: "); String carAgeString = in.readLine();		
			int carAge = Integer.parseInt(carAgeString);

			String query = String.format("INSERT INTO Car(vin, make, model, year) VALUES('%s', '%s', '%s', %d)", newCarVin, carMake, carModel, carAge);
            esql.executeUpdate(query);

      		}catch(Exception e){ System.err.println (e.getMessage()); }
	}
	
public static void InsertServiceRequest(MechanicShop esql){
		try{
			String querySize = String.format("SELECT rid FROM Service_Request");
			List<List<String>> ledgerOfServiceRequests = esql.executeQueryAndReturnResult(querySize);
			int newServiceRequest = ledgerOfServiceRequests.size() + 1;

            System.out.print("What is the Customer's Last name?: ") String findCustomerLastName = in.readLine();
             
			String query = String.format("SELECT * FROM Customer WHERE lname = '%s'", findCustomerLastName);
			int numOfCustomerRequests = esql.executeQueryAndPrintResult(query);

			if(!numOfCustomerRequests) {
				System.out.println("Hmmm, that last name does not belong to a current member, would they like to create a Membership? : ");
				String customerResponse = in.readLine();

                if (customerResponse.equals("y") || customerResponse.equals("Y") 
                    || customerResponse.equals("yes") || customerResponse.equals("Yes")) AddCustomer(esql);
				else { System.out.println("Maybe next time!");}
			}
			else {
				System.out.println("Enter the Member's ID number : "); String currentCustomerIDString = in.readLine();
				int currentCustomerID = Integer.parseInt(currentCustomerIDString);
				query = String.format("SELECT Row_Number() OVER ( ORDER BY Owns.car_vin ), Car FROM Customer,Owns,Car WHERE Customer.id = Owns.customer_id AND Car.vin = Owns.car_vin AND Owns.customer_id = '%s'", currentCustomerID);
				numOfCustomerRequests = esql.executeQueryAndPrintResult(query);
				System.out.println("Which Member do you have in mind? : ");
				String whichCustomerString = in.readLine();
				int whichCustomer = Integer.parseInt(whichCustomerString);
				
				if(!whichCustomer) {
					System.out.print("Enter new car's VIN: "); String newCarVin = in.readLine();
					System.out.print("Enter new car's make: "); String newCarMake = in.readLine();
					System.out.print("Enter new car's model: "); String newCarModel = in.readLine();
					System.out.print("Enter new car's year: "); String newCarAgeString = in.readLine();
					int newCarAge = Integer.parseInt(newCarAgeString);

					query = String.format("INSERT INTO Car(vin, make, model, year) VALUES('%s', '%s', '%s', %d)", newCarVin, newCarMake, newCarModel, newCarAge);
					esql.executeUpdate(query);
					
					querySize = String.format("SELECT ownership_id FROM Owns");
					List<List<String>> numCarsOwned = esql.executeQueryAndReturnResult(querySize);
					int newCarOwnedID = numCarsOwned.size() + 1;
					query = String.format("INSERT INTO Owns(ownership_id, customer_id, car_vin) VALUES(%d, %d, '%s')", newCarOwnedID, currentCustomerID, newCarVin);
					esql.executeUpdate(query);
					System.out.print("This member's new car has been added to the database!\n");

					System.out.print("Please Enter the current mileage of the new car: "); String newCarMileageString = in.readLine();
					int newCarMileage = Integer.parseInt(newCarMileageString);

					System.out.print("If the customer had any complaints about today's service, please describe here: "); String newServiceComplaint = in.readLine();
					
					query = String.format("INSERT INTO Service_Request(rid, customer_id, car_vin, date, odometer, complain) VALUES(%d, %d, '%s', CURRENT_DATE, %d, '%s')", newServiceRequest, currentCustomerID, newCarVin, newCarMileage, newServiceComplaint);
					esql.executeUpdate(query);
					System.out.print("This service request identification number will be given shortly. Thank you. : ");
					System.out.print(newServiceRequest);
					System.out.printf("%n"); 
				}
				else {

					query = String.format("SELECT test FROM (Select Row_Number() OVER ( ORDER BY Owns.car_vin ) as rownumber, Car.vin, Car.make, Car.model, Car.year FROM Customer, Owns, Car WHERE Customer.id = Owns.customer_id AND Car.vin = Owns.car_vin AND Owns.customer_id = '%s') AS test WHERE rownumber = %d ", currentCustomerID, whichCustomer);
					List<List<String>> thisCustomersCars  = esql.executeQueryAndReturnResult(query);
					
					String newCustomerCar = result.get(0).get(0);
					System.out.println(newCustomerCar);
					String[] carFormatting = newCustomerCar.split(",");
					String newCarVin2 = carFormatting[1];
					
					System.out.print("Please enter this car's mileage : "); String newCarMileageString2 = in.readLine();
					int newCarMileage2 = Integer.parseInt(newCarMileageString2);

					System.out.print("If the customer had any complaints about today's service, please describe here: "); String newServiceComplaint2 = in.readLine();

					query = String.format("INSERT INTO Service_Request(rid, customer_id, car_vin, date, odometer, complain) VALUES(%d, %d, '%s', CURRENT_DATE, %d, '%s')", newServiceRequest, currentCustomerID, newCarVin2, newCarMileage2, newServiceComplaint2);
					esql.executeUpdate(query);
					System.out.print("This service request identification number will be given shortly. Thank you. : ");
					System.out.print(newServiceRequest);
					System.out.printf("%n"); 

				}

			}

      		}catch(Exception e){ System.err.println (e.getMessage()); }

	}

public static void CloseServiceRequest(MechanicShop esql) throws Exception{
	try{
			String numClosedString = String.format("SELECT wid FROM Closed_Request");
			List<List<String>> numRequests = esql.executeQueryAndReturnResult(querySize);
			int newClosedRequestID = numRequests.size() + 1;

         	System.out.println("Please Enter the the Service Request ID for this Member's Car: "); String lookupRequestIDString = in.readLine();
			int lookupRequestID = Integer.parseInt(lookupRequestIDString);

			System.out.println("Please Enter your Mechanic's Union ID Number: "); String mechIDString = in.readLine();
			int mechID = Integer.parseInt(mechIDString);

			System.out.println("Please Enter any suggestions or comments pertaining to this Service Request: "); String newComment = in.readLine();
			System.out.println("Please enter the total cost of the Service Request, ready to be billed to the Member: "); String newServiceCostString = in.readLine();
			int newServiceCost = Integer.parseInt(newServiceCostString);

			String query = String.format("SELECT * FROM Mechanic WHERE Mechanic.id = %d", mechID);
			int numRows = esql.executeQuery(query);
			
			if(!numRows) {
				System.out.println("Please Enter a valid Mechanic Identification Number: \n");
				return;
			}
			
			query = String.format("SELECT * FROM Service_Request WHERE Service_Request.rid = %d", lookupRequestID);
			numRows = esql.executeQuery(query);

			if(!numRows) {
				System.out.println("Please Enter a valid Service Request Identification Number: \n");
				return;
			}

			query = String.format("SELECT * FROM Service_Request WHERE Service_Request.rid = %d AND Service_Request.date <= CURRENT_DATE", lookupRequestID);
			numRows = esql.executeQuery(query);

			if(!numRows) {
				System.out.println("Please enter a valid service request date: \n");
				return;
			}

			query = String.format("INSERT INTO Closed_Request(wid, rid, mid, date, comment, bill) VALUES(%d, %d, %d, CURRENT_DATE, '%s', %d)", 
										newClosedRequestID, lookupRequestID, mechID, newComment, newServiceCost);
			esql.executeUpdate(query);

      		}catch(Exception e){ System.err.println (e.getMessage()); }
    }
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){
		try{
				 String query = "SELECT Customer.fname, Customer.lname, Closed_Request.bill, Service_Request.date, Closed_Request.comment FROM Customer, Closed_Request, Service_Request 
				 					WHERE Closed_Request.bill < 100 AND Closed_Request.rid = Service_Request.rid AND Service_Request.customer_id = Customer.id";
         		int numCustomersLess100 = esql.executeQueryAndPrintResult(query);
				System.out.println ("Number of Customers with bills totaling less than $100: " + numCustomersLess100);
				 
      		}catch(Exception e){ System.err.println (e.getMessage()); }
	}

	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){
		try{
				String query = "SELECT allCars.fname, allCars.lname, allCars.numCars FROM (SELECT Owns.customer_id, Customer.fname, Customer.lname, COUNT(*) numCars FROM Owns,Customer 
								WHERE Customer.id = Owns.customer_id GROUP BY Owns.customer_id, Customer.fname, Customer.lname) AS allCars WHERE numCars > 20";
				int numCustomersMore20 = esql.executeQueryAndPrintResult(query);
				System.out.println ("Number of Customers with more than 20 cars: " + numCustomersMore20);
		 
			}catch(Exception e){ System.err.println (e.getMessage()); }

	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){
		try{
				 String query = "SELECT Car.make, Car.model, Car.year, Service_Request.odometer FROM Car,Service_Request 
				 					WHERE Service_Request.car_vin = Car.vin AND Service_Request.odometer < 50000 AND Car.year < 1995";
         		int numCarsBefore1995 = esql.executeQueryAndPrintResult(query);
				 System.out.println ("Number of Cars manufactured before 1995 with more than 50,000 miles: " + numCarsBefore1995);
				 
      		}catch(Exception e){ System.err.println (e.getMessage()); }
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){
	try{
		System.out.print("How many service requests will you consider for this criteria?: "); String numServRequestsString = in.readLine();
		int numServRequests = Integer.parseInt(numServRequestsString);
	
		System.out.print("How many cars would you like listed, for this criteria?: "); String numCarsListedString = in.readLine();
		int numCarsListed = Integer.parseInt(numCarsListedString);

		String query = String.format("SELECT Car.make, Car.model, Car.vin, COUNT(Service_Request) as cnt FROM Car,Service_Request 
										WHERE Service_Request.rid NOT IN (SELECT Service_Request.rid FROM Closed_Request,Service_Request WHERE Service_Request.rid = Closed_Request.rid) AND Car.vin = Service_Request.car_vin 
										GROUP BY Car.make,Car.model,Car.vin HAVING COUNT(*) = %d ORDER BY cnt DESC LIMIT %d",numServRequests,numCarsListed);

		int totalValidRequests = esql.executeQueryAndPrintResult(query);
		 System.out.println ("The number of cars that fit this criteria: " + totalValidRequests);
		 
	}catch(Exception e){ System.err.println (e.getMessage()); }

	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){
	try{
		String query = "SELECT A.fname,A.lname, SUM(Closed_Request.bill) total_bill FROM (SELECT Customer.fname, Customer.lname, Customer.id, Closed_Request.bill, Closed_Request.rid FROM Customer, Closed_Request, Service_Request 
						WHERE Closed_Request.rid = Service_Request.rid AND Customer.id = Service_Request.customer_id) AS A LEFT JOIN Closed_Request ON A.rid = Closed_Request.rid GROUP BY A.fname,A.lname,A.id ORDER BY total_bill DESC";

		int numCustomers = esql.executeQueryAndPrintResult(query);
		 System.out.println ("The number of customers who fit this criteria: " + numCustomers);
		 
	}catch(Exception e){ System.err.println (e.getMessage()); }

	
	}
	
}