This challenge is used by MediaMath for evaluating candidates to data engineering positions.

# Background
Advertisers want to understand how digital advertising generates interactions with their brands and websites: clicks, visits, purchases, etc. MediaMath collects such interactions and ties them to the ads that are displayed online through a process called attribution.

# The challenge

## Product specification
The goal of this challenge is to write a simple attribution application that produces a report that can then be fed into a database.

This application will run the following two operations on the provided datasets.

### Attribution
The application will process input datasets that represent interactions with brands ("events") and ads displayed online ("impressions") to output some simple statistics:

 - The count of events for each advertiser, grouped by event type.
 - The count of unique users for each advertiser, grouped by event type.

### De-duplication
Events are sometimes registered multiple times in the events dataset when they actually should be counted only once. For instance, a user might click on an ad twice by mistake.

When running the attribution process we want to de-duplicate these events: for a given user / advertiser / event type combination, we want to remove events that happen more than once every minute.

## Inputs
Two datasets in CSV format are provided for this challenge. Their schemas are provided below.

### Events
This dataset contains a series of interactions of users with brands.

**Schema:**

Column number | Column name  | Type | Description
------------- | ------------- | ------------- | -------------
1  | timestamp | integer | Unix timestamp when the event happened.
2  | advertiser_id | integer | The advertiser ID that the user interacted with.
3 | user_id | string (UUIDv4) | An anonymous user ID that generated the event.
4 | event_type | string | The type of event. Potential values: click, visit, purchase

### Impressions
This dataset contains a series of ads displayed to users online for different advertisers.

**Schema:**

Column number | Column name  | Type | Description
------------- | ------------- | ------------- | -------------
1  | timestamp | integer | Unix timestamp when the impression was served.
2  | advertiser_id | integer | The advertiser ID that owns the ad that was displayed.
3 | creative_id | integer | The creative (or ad) ID that was displayed.
4 | user_id | string (UUIDv4) | An anonymous user ID this ad was displayed to.

## Outputs
The attribution application must process the provided datasets and produce the following two CSV files as its output.

### Count of events
The first file must be named `count_of_events.csv` and will contain the **count of events for each advertiser, grouped by event type**.

**Schema:**

Column number | Column name  | Type | Description
------------- | ------------- | ------------- | -------------
1  | advertiser_id | integer | The advertiser ID
2 | event_type | string | The type of event. Potential values: click, visit, purchase
3 | count | int | The count of events for this advertiser ID and event type.

### Count of unique users
The second file must be named `count_of_users.csv` and will contain the **count of unique users for each advertiser, grouped by event type**.

**Schema:**

Column number | Column name  | Type | Description
------------- | ------------- | ------------- | -------------
1 | advertiser_id | integer | The advertiser ID
2 | event_type | string | The type of event. Potential values: click, visit, purchase
3 | count | int | The count of unique users for this advertiser ID and event type.

# Rules of the game
This challenge is a chance for MediaMath engineers to see how you code and organize a project to implement a specification.

## Deliverables
The expected deliverable is a fully functional project that includes the following:

 - Code of the application.
 - Test suite for the application.
 - Documentation for launching a development environment and running the application.
 - Output files (`count_of_events.csv` and `count_of_users.csv`) generated by your application when you run it.

## Technical stack
The application must use the following technologies:

 - Java **or** Scala **or** Python
 - Hadoop **or** Spark

Except for these requirements, feel free to use whichever libraries, frameworks or tools you deem necessary. 

## Expectations
Your code will be reviewed by multiple engineers and can serve as the base for a discussion in interviews.
We want to see how you approach working on a complete project and strongly recommend that you work on this challenge alone.

Feel free to ask any question that might help you understand the specifications or requirements better (as you would in your work) if anything is unclear.

## Delivery
Your application can be sent to us through a GitHub repository (in which case you are welcome to fork this repository) or as a compressed archive containing all the deliverables. 