#! /bin/bash
DBNAME=$ryuzu001_DB
PORT=$9998
USER=$ryuzu001

# Example: source ./run.sh flightDB 5432 user
java -cp lib/*:bin/ MechanicShop $DBNAME $PORT $USER
