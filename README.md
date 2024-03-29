<img src="./logo.png">

[![Tests](https://github.com/MattBast/anasto/actions/workflows/tests.yml/badge.svg)](https://github.com/MattBast/anasto/actions/workflows/tests.yml)

Sync data from source system tables into destination lakes, warehouses and databases with nothing more than a config file.

> Note: Anasto has been started as a personal project and is very much in the early stages. I hope you like it and I would love any feedback or ideas you have but please don't expect a feature rich application that is ready for production (yet :crossed_fingers:).

Anasto is a middleman synchronising source and destination data stores. It will listen to one or more sources and copy all the data it finds to one or more destinations. Anasto favours near realtime synchronisation meaning that changes in sources systems should be "heard" quickly and copied to the destination systems soon after.

```mermaid
flowchart LR
    %% nodes
    subgraph source_one
    src_table_one[table_one]:::table;
    end

    subgraph source_two
    src_table_two[table_two]:::table;
    end

    subgraph source_three
    src_table_three[table_three]:::table;
    end
    
    anasto[Anasto];

    subgraph destination_one
    dest_table_one[table_one]:::table;
    end

    subgraph destination_two
    dest_table_two[table_two]:::table;
    dest_table_three[table_three]:::table;
    end

    %% links
    src_table_one-->anasto;
    src_table_two-->anasto;
    src_table_three-->anasto;
    anasto-->dest_table_one;
    anasto-->dest_table_two;
    anasto-->dest_table_three;
    
    %% styling
    style anasto fill:#1a1a1a,stroke:#1a1a1a,stroke-width:40px,color:#ffffff;
    classDef table fill:#808080,stroke:#fff,color:#ffffff;
```
<sub>Note: If this diagram isn't rendering in your markdown viewer, copy and paste the code into this [editor](https://mermaid.live/).</sub>


## Quickstart
Make sure [Rust](https://www.rust-lang.org/tools/install) is installed on your machine. Make sure the Rust toolchain is installed by running this command on your command line (you may need to restart your command line after installing Rust for this command to work):

```bash
cargo --version
```

Now you'll need to create a config file to specify what sources of data you'd like to read and what destinations that data should be written to. Create your file with the command `touch ./config.toml`. Open the file and add this content to it:

```toml
[[source_table]]
type = "files"
table_name = "csv_table"
dirpath = "./csv_table/"

[[destination_table]]
type = "files"
source_table_name = "csv_table"
dest_table_name = "json_table"
dirpath = "./json_table/"
filetype = "json"
```

This file reads from csv files found in the `./csv_table/` directory and writes them as json files in the `./json_table/` directory. For more information on what can be included in a config file, see this section: [The Config File](#the-config-file). 

Now create the two directories for the tables:
```bash
mkdir ./csv_table/
mkdir ./json_table/
```

And finally start Anasto and point it at your config file:
```bash
cargo run ./config.toml
```

Anasto will start polling `./csv_table/` looking for new files to read. Try adding some csv files to the directory to see them copied across to the `./json_table/` directory. Note that Anasto expects only one table per directory so all the csv files you add will need a compatible schema (i.e. all the values added to a column with the same column across the files needs the same data type).

## The Config File
Anasto is configured via a single toml file. This [introduction page](https://toml.io) is worth a read if you haven't worked with toml files before.

The config file consists of a list of tables written in the [toml format](https://toml.io/en/v1.0.0#table). Each table represents a single table of data either in a source system that you'd like to read from or a destination that you'd like to write to. You can specify whether a table is a source or a destination using these headers surrounded by two sets of square brackets:

```toml
[[source_table]]

[[destination_table]]
```

The details of the tables are then included under these headers. All tables include the `type` field to specify what type of table you are reading from or writing to. In the example below we are reading and writing to files on the local filesystem. Notice as well that the `destination_table` needs to specify which `source_table` it is synchronising with by specifying the `source_table` name in its `source_table_name` field:

```toml
[[source_table]]
type = "files"
table_name = "csv_table"
dirpath = "./csv_table/"

[[destination_table]]
type = "files"
source_table_name = "csv_table"
dest_table_name = "json_table"
dirpath = "./json_table/"
filetype = "json"
```

Now let's get into the details of each possible source and destination type currently available in Anasto:

### Files (Source Table) 
A source table that is read from the local filesystem. It works by polling a specified file directory and reading all the files that have been created since Anasto polled the directory. A timestamp bookmark is kept and compared against a files created timestamp to track what files need to be read per poll.

Here's an example:
```toml
[[source_table]]
type = "files"
table_name = "csv_table"
dirpath = "./csv_table/"
filetype = "csv"
bookmark = "2023-08-21T00:55:00z"
poll_interval = 10000
on_fail = "stop"
```

And here's a description of each of the fields:

| Field Name    | Data Type      | Description                                                                                                                                                                                                                     |
|---------------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table_name    | String         | A user defined name for the table. This does not need to correlate with the directory path where the tables files are stored.                                                                                                   |
| dirpath       | Path           | The directory filepath where all data in this table is stored.                                                                                                                                                                  |
| filetype      | Enum           | Optional field. The type of file that the tables data is stored in. Available formats are `csv`, `json`, `avro` and `parquet`. Defaults to `csv`.                                                                               |
| bookmark      | Datetime (UTC) | Optional field. Filter out any files whose created date is earlier than this bookmark. Defaults to "1970-01-01T00:00:00z".                                                                                                      |
| poll_interval | int          | Optional field. Determines how frequently new data will be read from the source. Provided in milliseconds. Defaults to 10000.                                                                                             |
| on_fail       | Enum           | Optional field. Decide what to do when new data fails to be read from the source. Available values are `stop` (stops Anasto reading the table) and `skip` (Anasto skips the batch with an error in it). Defaults to `stop`. |

### API (Source Table) 
A source table that is read from an API. It works by calling a specified API endpoint either once or polled continuously and sending all selected data in the response to be written as a table. A bookmark can be inserted into the request to request only new data. Pagination can also be configured for endpoints that return more data than can be received in a single request.

Here's an example calling the [Pokeapi](https://pokeapi.co/) API:
```toml
[[source_table]]
type = "api"
table_name = "pikachu_source"
endpoint_url = "https://pokeapi.co/api/v2/pokemon/pikachu"
one_request = true
```

And here's a description of each of the fields:

| Field Name                 | Data Type                | Description                                                                                                                            |
|----------------------------|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| table_name                 | String                   | A user defined name for the table. This does not need to correlate with the directory path where the tables files are stored.          |
| endpoint_url               | String                   | The url of the endpoint to call.                                                                                                       |
| method                     | Enum                     | The HTTP method to call this endpoint with. Can be one of: get, post, put, patch, delete.                                              |
| select_field               | Array                    | Select the field in the response where the table data resides. Provide as a vector of strings to select a nested field                 |
| timeout                    | int                      | How long to wait for a response before cancelling the request (in seconds)                                                             |
| query                      | Array of tuples          | Adds one or more queries to the url                                                                                                    |
| basic_auth                 | Tuple of strings.        | Adds a basic (username-password) header to the request                                                                                 |
| headers                    | Array of tuples          | Adds one or more headers to the request                                                                                                |
| body                       | Object                   | Adds a json body to the request                                                                                                        |
| bookmark                   | String                   | Tracks which files have been read using their created timestamp                                                                        |
| bookmark_key               | String                   | Optional field. State the name of the key of the bookmark in the API request.                                                          |
| bookmark_location          | String                   | Optional field. State where the bookmark cursor should be placed in the request.                                                       |
| bookmark_format            | String                   | Optional field. State what datetime format the bookmark should be in when it is added to an API request.                               |
| poll_interval              | int                      | Optional field. Determines how frequently new data will be written to the destination. Provided in milliseconds.                       |
| on_fail                    | Enum                     | Optional field. Decide what to do when new data fails to be written to a destination. Can be one of: skip, stop.                       |
| one_request                | bool                     | Optional field. State if this source should call an API just once (true) or if it should poll the API (false). Defaults to false.      |
| pagination                 | Enum                     | Optional field. States what pagination approach will be taken. Can be one of: none, page_increment, offset_increment, cursor Defaults to none meaning no pagination will be performed. |
| pagination_page_token_key  | String                   | Optional field. States the name of the page number or offset parameter that will increment during pagination. Defaults to "page_size". |
| pagination_page_number     | int                      | Optional field. Keeps track of what page the pagination needs to request. Defaults to 0.                                               |
| pagination_page_size_key   | String                   | Optional field. States the name of the page size parameter that will increment during pagination. Defaults to "page".                  |
| pagination_page_size       | int                      | Optional field. States how many records will be returned per page during pagination. Defaults to 5.                                    |
| max_pagination_requests    | int                      | Optional field. States the maximum number of requests a paginated call can make. Defaults to 100.                                      |
| pagination_offset          | int                      | Optional field. Keeps track of how many records the pagination requests have received. Defaults to 0.                                  |
| pagination_cursor_field    | Array of strings         | Optional field. Select the field that will be used to request the next page of results.                                                |
| pagination_cursor_record   | Enum                     | Optional field. Define which record in a response contains the cursor. Defaults to the last record. Can be one of: first, last         |
| pagination_cursor_location | Enum           | Optional field. State where the pagination cursor should be placed in the request.  Can be one of: body, header.                                 |

Here's some more examples for how to configure the API source. The first adds query parameters to a request to the [Open Meteo](https://open-meteo.com/) endpoint:
```toml
[[source_table]]
type = "api"
table_name = "weather_source"
endpoint_url = "https://api.open-meteo.com/v1/forecast"
query = [
    ["latitude", "52.52"],
    ["longitude", "13.41"],
    ["current", "temperature_2m,wind_speed_10m"],
    ["hourly", "temperature_2m,relative_humidity_2m,wind_speed_10m"]
]
one_request = true
```

And this one uses pagination (page increment approach) to make multiple requests to the [Regres](https://reqres.in/) users endpoint:
```toml
[[source_table]]
type = "api"
table_name = "users_source"
endpoint_url = "https://reqres.in/api/users"
one_request = true
pagination = "page_increment"
pagination_page_token_key = "page"
pagination_page_number = 1
pagination_page_size_key = "per_page"
pagination_page_size = 5
max_pagination_requests = 3
select_field = ["data"]
```

### Open Table (Source Table) 
A source table that is read read from a local open table database like [Delta Lake](https://delta.io/). It works by polling the change data feed files and reading a stream of these change events. Note that for now this table only supports the delta lake format. The Iceberg format will be added once the [Apache Iceberg crate](https://github.com/apache/iceberg-rust) matures.

Here's an example:
```toml
[[source_table]]
type = "files"
table_name = "delta_table"
dirpath = "./delta_table/" 
format = "delta_lake"
bookmark = "2023-08-21T00:55:00z"
poll_interval = 5000
on_fail = "skip"
```

And here's a description of each of the fields:

| Field Name    | Data Type      | Description                                                                                                                                                                                                                 |
|---------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table_name    | String         | A user defined name for the table. This does not need to correlate with the directory path where the tables files are stored.                                                                                               |
| dirpath       | Path           | The directory filepath where all data in this table is stored.                                                                                                                                                              |
| format        | Enum           | Optional field. The open table format that the tables data is stored in. The only format available is `delta_lake`. Defaults to `delta_lake`.                                                                               |
| bookmark      | Datetime (UTC) | Optional field. Filter out any change feed events whose committed date is earlier than this bookmark. Defaults to "1970-01-01T00:00:00z".                                                                                   |
| poll_interval | int          | Optional field. Determines how frequently new data will be read from the source. Provided in milliseconds. Defaults to 10000.                                                                                               |
| on_fail       | Enum           | Optional field. Decide what to do when new data fails to be read from the source. Available values are `stop` (stops Anasto reading the table) and `skip` (Anasto skips the batch with an error in it). Defaults to `stop`. |

### Files (Destination Table) 
A destination table that is written to the local filesystem. It writes one or more files of data to the specified directory for every new batch of data it receives from source.

Here's an example:
```toml
[[destination_table]]
type = "files"
source_table_name = "csv_table"
dest_table_name = "json_table"
dirpath = "./json_table/"
filetype = "json"
on_fail = "skip"
```

And here's a description of each of the fields:

| Field Name        | Data Type | Description                                                                                                                                                                                                                         |
|-------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dest_table_name   | String    | A user defined name for the table. This does not need to correlate with the directory path where the tables files will be written to.                                                                                               |
| source_table_name | String    | The name of the source table that supplies this destination table. There must be a `source_table` in the config file with this table name.                                                                                          |
| dirpath           | Path      | The directory filepath where all data in this table will be written to.                                                                                                                                                             |
| filetype          | Enum      | Optional field. The type of file that the tables data will be written to. Available formats are `csv`, `json`, `avro` and `parquet`. Defaults to `csv`.                                                                             |
| on_fail           | Enum      | Optional field. Decide what to do when new data fails to be written to a destination. Available values are `stop` (stops Anasto writing to this table) and `skip` (Anasto skips the batch with an error in it). Defaults to `stop`. |


### Open Table (Destination Table) 
A destination table that is written to a local open table database like [Delta Lake](https://delta.io/). It writes one or more files of data to the specified directory for every new batch of data it receives from source. Note that for now this table only supports the delta lake format. The Iceberg format will be added once the [Apache Iceberg crate](https://github.com/apache/iceberg-rust) matures.

Here's an example:
```toml
[[destination_table]]
type = "files"
source_table_name = "json_table"
dest_table_name = "delta_table"
dirpath = "./delta_table/"
format = "delta_lake"
on_fail = "skip"
```

And here's a description of each of the fields:

| Field Name        | Data Type | Description                                                                                                                                                                                                                         |
|-------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dest_table_name   | String    | A user defined name for the table. This does not need to correlate with the directory path where the tables files will be written to.                                                                                               |
| source_table_name | String    | The name of the source table that supplies this destination table. There must be a `source_table` in the config file with this table name.                                                                                          |
| dirpath           | Path      | The directory filepath where all data in this table will be written to.                                                                                                                                                             |
| format            | Enum      | Optional field. The open table format that the tables data is stored in. The only format available is `delta_lake`. Defaults to `delta_lake`                                                                                        |
| on_fail           | Enum      | Optional field. Decide what to do when new data fails to be written to a destination. Available values are `stop` (stops Anasto writing to this table) and `skip` (Anasto skips the batch with an error in it). Defaults to `stop`. |

## Where the name comes from
The name is short for the word anastomosis which according to [Wikipedia](https://en.wikipedia.org/wiki/Anastomosis) "is a connection or opening between two things (especially cavities or passages) that are normally diverging or branching, such as between blood vessels, leaf veins, or streams." This sort of describes what the tool does which is take an event as input and output it to one or more destinations. More than that though the name is quite unique and vague enough to allow flexibility in the tools future features.