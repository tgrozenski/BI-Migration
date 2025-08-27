### Written by Tyler Grozenski for CS5200 ###
### grozenski.t@northeastern.edu ###
### Practicum II ###
### 8/5/2025 ###

create_fact_table <- function(cloud_con) {
    dbExecute(cloud_con, "
        CREATE TABLE IF NOT EXISTS FilmMusicFacts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            TotalRevenue DECIMAL(10, 2),
            TotalUnitsSold INTEGER,
            MinSpent DECIMAL(10, 2),
            MaxSpent DECIMAL(10, 2),
            AvgSpent DECIMAL(10, 2),
            Year VARCHAR(255),
            Month VARCHAR(255),
            Quarter INTEGER,
            Country VARCHAR(255),
            Medium VARCHAR(255)
        );
    ")
}

create_dimension_tables <- function(cloud_con) {
    dbExecute(cloud_con, "
        CREATE TABLE IF NOT EXISTS FilmMusicCustomers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            NumCust INTEGER,
            Country VARCHAR(255),
            Medium VARCHAR(255)
       );
    ")
}

main <- function() {
    library(DBI)
    library(RMySQL)

    hostname <- "mysql-practicum1-tgrozenski-practicum1-tg.g.aivencloud.com"
    port <- 25306
    username <- "avnadmin"
    password <- "AVNS_fg_Htu4YZyLpVQ2H6g6"
    dbname <- "defaultdb"

    cloud_con <- dbConnect(RMySQL::MySQL(),
                        host = hostname,
                        port = port,
                        user = username,
                        password = password,
                        dbname = dbname,
                        timeout = 10)

    if (dbIsValid(cloud_con)) {
        print("SUCCESS")
    }

    # Create teh fact and dimension tables
    create_fact_table(cloud_con)
    create_dimension_tables(cloud_con)

    dbDisconnect(cloud_con)

}
main()