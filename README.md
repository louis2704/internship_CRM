# internship_CRM
Hi everyone,

I did my end-of-studies placement with a Paris-based ESN, with the aim of becoming a data engineer and data analyst on the solutions offered by Microsoft (Azure Data, Power Platform, MS SQL Server, SSIS, etc.). At the request of some of the company's sales people, I was then asked to develop an internal CRM on Power Apps, based on a raw extraction of contacts from the sales people's LinkedIn accounts, to facilitate customer and talent prospecting. 

However, LinkedIN has a good system for protecting personal data and the extraction did not include any professional email addresses! 
So my goal was to create an e-prospecting CRM with a database that contained no emails! 

Thanks to scraping on sites that provide company email formats, a bulk generation method and the use of Apollo's very precise API, I managed to collect more than 75% of valid addresses, i.e. around 50,000! 
I then deployed the code on Azure Functions to make the database development process automatic: all you have to do is upload your LinkedIN contact extractions and a few minutes later, your contacts will have a location and a business email address assigned to them and will be available directly on the CRM interface. 


