# NYPD Officer complaint data
- Source: 
  - Repo structure based on: https://github.com/konosp/dbt-airflow-docker-compose
  - Data from: https://github.com/ryanwatkins/nypd-officer-profiles 
- Example Uses:
  - Analytics: 
    - How many active officers on the force year over year?
    - How many complaints did officers have on average each year?
    - What % of complaints end in an unfavorable outcome for the officer?
    - Are # of arrests correlated with complaints?
    
  - Machine learning
    - Can we predict which officers will go on to have adverse events based on their history?
    - Can past complaints predict future lawsuits?


## Helpful reminder for debuggins
```
# To test database commands
docker exec -it nypd-db-postgres-dbt-1 psql -U dbtuser -W dbtdb

# Airflow DAGs
docker exec -it dbt-airflow-docker_airflow_1 /bin/bash
```


