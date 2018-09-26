# Rdb - connector - redshift [![Codeship Status for emartech/rdb-connector-redshift](https://app.codeship.com/projects/b0a10590-9c55-0135-18fe-3ec7b5d83301/status?branch=master)](https://app.codeship.com/projects/252931) [![](https://jitpack.io/v/emartech/rdb-connector-redshift.svg)](https://jitpack.io/#emartech/rdb-connector-redshift)

## Definitions:

**Router** - instantiates a specific type of connector
 
**Database connector** - implements an interface, so that the router can be connected to the specific type of database.

## Tasks:

Implements the general database connector trait, and contains redshift
 specific implementation. For testing, it uses the tests written in rdb - connector - common - test, applied for the redshift connector.

## Dependencies:

**[Rdb - connector - common](https://github.com/emartech/rdb-connector-common)** - defines a Connector trait, that should be implemented by different types of connectors. Contains the common logic, case classes and some default implementations, that may be overwritten by specific connectors, and may use functions implemented in the connectors. (eg. validation)


**[Rdb - connector - test](https://github.com/emartech/rdb-connector-test)**  - contains common test implementations, that may be used in specific connectors

## Testing:

To test the application, you need to have the correct environment variables set to use your redshift db. You can also define these variables
in a .env file.


