Assignment: Lastest-value price service
Version: 2.2 .1

The purpose of this exercise is for you to demonstrate that you can analyse business requirements and convert them into clean code.
Please apply the same standards to the code which you would if this was a production system.
Comments in the code to illustrate design decisions are highly appreciated.

Business requirements:

Your task is to design and implement a service for keeping track of the last price for financial instruments.
Producers will use the service to publish prices and consumers will use it to obtain them.

The price data consists of records with the following fields:
* id: a string field to indicate which instrument this price refers to.
* asOf: a date time field to indicate when the price was determined.
* payload: the price data itself, which is a flexible data structure.

Producers must upload prices in batches to ensure data consistency. The sequence of uploading a batch is as follows:
1. The producer indicates that a batch is started
2. The producer uploads the price records for the batch in parallel in multiple chunks of 1000 records each.
3. The producer completes or cancels the batch

On completion, all prices in a batch should be made available for consumption at the same time in an atomic fashion. 
Batch which are cancelled can be discarded entirely.

Consumers can request the last price records for a given list of ids. 
The last value is determined by the asOf time, as set by the producer, and not the order they came in.
Consumers requesting data should not see any partial data from incomplete batches. 
They should only see data from previously completed batches.

The service should be resilient against producers which call the service methods in an incorrect order,
or clients which call the service while a batch is being processed.

Technical requirements:

* Provide a working Java application which implements the business requirements
* The service interface should be defined as a Java API, in a single class, so consumers and producers can be assumed to run in the same JVM.
* For the purpose of the exercise, we are looking for an in-memory implemenentation of the service (rather than one that relies on a database), using core java constructs only (Java SDK).
* Demonstrate the functionality of the application through unit tests
* Please include gradle or maven project definitions

Bonus points for simplicity!
Strive for a solution that is clean, simple, and performant. The most elegant designs often are. 
This assignment should not take more than 2-3 hours to complete.





