## Run steps

1. run confluent kafka

   `cd confluent`

   `docker-compose up -d`

2. Create topics from control center (@ localhost:9021):
    - Customer: 2 partitions
    - Balance: 2 partitions
    - CustomerBalance: 2 partitions
3. Create schema for each topic in refer to .avsc file under folder

   `src/main/resources/avro`
4. run avro stream demo

   `cd ..`

   `./mvnw spring-boot:run`

