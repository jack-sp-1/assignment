# assignment
to check for relations between customers and other table

How to test :
1) Run it as is in some editor where spark  is running.
or

2)Run the first_shell.sh script after giving queue name.


glue   -- This all transformations can be taken care using glue in AWS.
questions--
1) I have calculated loyalty flag based on below criteria:
	a) for all the customer id, calculated the total amount
	b) if total amount calculated in step a) is more than 1000 for all the months  ,then  customer is loyalty
	c) else customer is not loyal.
jan feb march 
1000 1000 1000      ## question  1  === loyalty flag   should be based on all months

2) for change in prices for Wednesday
change in taxes 
 tax can change to 5 percent if required and unit price can be adjusted based on number of items saying special price
 

linkage attack 
1) It can be avoided by using encryption while doing transformations and decryption while saving it in tables.
2) glue PII transforms -- they can be applied to mask some of the PII columns like state , age .
