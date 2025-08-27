### Written by Tyler Grozenski for CS5200 ###
### grozenski.t@northeastern.edu ###
### Practicum II ###
### 8/5/2025 ###

# These are the Queries to pull data from source dbs into a format
# that we can push to the new mySQL database
music_facts_query <- "
        WITH Invoice_Counts AS (
            SELECT
                SUM(ii.Quantity) AS TotalUnitsSold,

                -- Aggregation
                strftime('%Y', i.InvoiceDate) AS Year,
                strftime('%m', i.InvoiceDate) AS Month,
                ((CAST(STRFTIME('%m', i.InvoiceDate) AS INTEGER) - 1) / 3 + 1 ) AS Quarter,
                i.BillingCountry AS Country

                FROM
                    invoices i
                JOIN 
                    invoice_items ii ON i.InvoiceId = ii.invoiceId
                GROUP BY Country, Year, Month, Quarter
        ),
        Invoice_Revenue AS (
            SELECT
                -- Facts
                SUM(i.Total) AS TotalRevenue,
                AVG(i.Total) AS AverageAmount,
                MIN(i.Total) AS MinRevenue,
                MAX(i.Total) AS MaxRevenue,

                -- Aggregation
                strftime('%Y', InvoiceDate) AS Year,
                strftime('%m', InvoiceDate) AS Month,
                ((CAST(STRFTIME('%m', InvoiceDate) AS INTEGER) - 1) / 3 + 1 ) AS Quarter,
                i.BillingCountry AS Country
            FROM
                invoices i

            GROUP BY Country, Year, Month, Quarter
        )

        SELECT
            -- Facts
            ir.TotalRevenue,
            ic.TotalUnitsSold,
            ir.MinRevenue AS MinSpent,
            ir.MaxRevenue AS MaxSpent,
            ir.AverageAmount AS AvgSpent,

            -- Aggregation
            ic.Year,
            ic.Month,
            ic.Quarter,
            ic.Country,
            'Music' AS Medium

        FROM Invoice_Counts ic

        JOIN 
            Invoice_Revenue ir ON
                ic.Country = ir.Country
                AND ic.Year = ir.Year
                AND ic.Month = ir.Month
                AND ic.Quarter = ir.Quarter
"

film_facts_query <- "
    SELECT
        -- Facts
        SUM(p.amount) AS TotalRevenue,
        COUNT(r.rental_id) AS TotalUnitsSold,
        MIN(p.amount) AS MinSpent,
        MAX(p.amount) AS MaxSpent,
        AVG(p.amount) AS AvgSpent,
 
        -- Aggregation
        strftime('%Y', p.payment_date) AS Year,
        strftime('%m', p.payment_date) AS Month,
        ((CAST(STRFTIME('%m', p.payment_date) AS INTEGER) - 1) / 3 + 1 ) AS Quarter,
        co.country AS Country,
        'Film' AS Medium
    FROM
        payment p 
    JOIN
        rental r ON p.rental_id = r.rental_id
    JOIN 
        customer c ON p.customer_id = c.customer_id
    JOIN 
        address a ON c.address_id = a.address_id
    JOIN 
        city ci ON a.city_id = ci.city_id
    JOIN 
        country co ON ci.country_id = co.country_id
    GROUP BY co.country, Year, Month, Quarter;
"

music_customers_query <- "
    SELECT
        COUNT(DISTINCT CustomerId) AS NumCust,
        Country,
        'Music' AS Medium

    FROM customers GROUP BY Country;
"

film_customers_query <- "
    SELECT
        COUNT(DISTINCT customer_id) NumCust,
        Country,
        'Film' AS Medium

    FROM customer c
    
    JOIN 
        address a ON c.address_id = a.address_id
    JOIN 
        city ci ON a.city_id = ci.city_id
    JOIN 
        country co ON ci.country_id = co.country_id
    GROUP BY Country;
"

# This function executes a query in batches
# Depends on df_to_sql to turn the batch into a sql string
query_to_db <- function(src_con, dest_con, query, dest_table_name, batch_size = 200) {

    res <- dbSendQuery(src_con, query)

    while (!dbHasCompleted(res)) {
        batch <- dbFetch(res, n = batch_size)
        if (nrow(batch) > 0) {
            dbExecute(dest_con, df_to_sql(batch, dest_table_name))
            print("Batch Complete")
        }
    }
    dbClearResult(res)

  message("All batches for table", dest_table_name, "complete")
}

# Takes a dataframe and formats it into a valid sql insert statement
df_to_sql <- function(df, table_name) {
    print(head(df))
    order <- paste(paste0(colnames(df)), collapse = ", ")

    # Wrap dates and strings in single quotes, round numbers
    values <- character(nrow(df))
    for (i in seq(nrow(df))) {

        row_as_list <- as.list(df[i, ])
        processed_elements <- sapply(row_as_list, function(element) {
            # wrap text/dates in '', round numbers
            if (!is.numeric(element) || inherits(element, "Date")) {
                return(paste0("'", element, "'"))
            } else {
                return(round(element, digits = 2))
            }
        })

        # Collapse and store the result
        values[i] <- paste0("(", paste(processed_elements,
        collapse = ", "), ")")
    }

    # formatted statement to return
    sql <- sprintf("INSERT INTO %s (%s) VALUES %s;", table_name, order, paste(values, collapse = ", "))
    return(sql)
}

main <- function() {
    library(DBI)
    library(RMySQL)

    hostname <- "mysql-practicum1-tgrozenski-practicum1-tg.g.aivencloud.com"
    port <- 25306
    username <- "avnadmin"
    password <- "AVNS_fg_Htu4YZyLpVQ2H6g6"
    dbname <- "defaultdb"

    # Establish all 3 database connections
    cloud_con <- dbConnect(RMySQL::MySQL(),
                        host = hostname,
                        port = port,
                        user = username,
                        password = password,
                        dbname = dbname,
                        timeout = 10)

    film_db_path <- "src_data/film-sales.db"
    film_con <- dbConnect(RSQLite::SQLite(), dbname = film_db_path)

    music_db_path <- "src_data/music-sales.db"
    music_con <- dbConnect(RSQLite::SQLite(), dbname = music_db_path)

    # Verify connections
    if (dbIsValid(cloud_con) && dbIsValid(film_con) && dbIsValid(music_con)) {
        print("SUCCESS FOR ALL 3 CONNECTIONS")
    }

    # Migration from film-sales.db -> defaultdb FilmMusicFacts table
    query_to_db(
        src_con = film_con,
        dest_con = cloud_con,
        query = film_facts_query,
        dest_table_name = "FilmMusicFacts"
    )
    print("Film Facts Migrated")

    # Migration from music-sales.db -> defaultdb FilmMusicFacts table
    query_to_db(
        src_con = music_con,
        dest_con = cloud_con,
        query = music_facts_query,
        dest_table_name = "FilmMusicFacts"
    )
    print("Music Facts Migrated")

    # Migration from music-sales.db -> defaultdb FilmMusicCustomers table (Dimension Table)
    query_to_db(
        src_con = music_con,
        dest_con = cloud_con,
        query = music_customers_query,
        dest_table_name = "FilmMusicCustomers"
    )
    print("Music dim Migrated")

    # Migration from film-sales.db -> defaultdb FilmMusicCustomers table (Dimension Table)
    query_to_db(
        src_con = film_con,
        dest_con = cloud_con,
        query = film_customers_query,
        dest_table_name = "FilmMusicCustomers"
    )
    print("Film dim Migrated")

    # Resolve discrepancy between USA and United States
    update_film_to_usa <- "UPDATE FilmMusicFacts SET country = 'USA'
        WHERE country = 'United States';"
    dbExecute(cloud_con, update_film_to_usa)

    # Disconnect All
    dbDisconnect(cloud_con)
    dbDisconnect(film_con)
    dbDisconnect(music_con)
}
main()