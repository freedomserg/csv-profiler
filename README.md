## CSV Profiler

CSV Profiler reads a csv file from the local filesystem, processes and verifies persisted data, and provides profiling information:
total number of unique values, count of each unique value for each column. Profiling info outputted in the console and stored
in the file with name ending with `_profile` stored in the same source directory.

### Input

CSV Profiler accepts the following command-line opts:
- `--dir` (required): a source directory in the local filesystem where a csv file resides.
- `--file` (required): a source file name.
- `--column` (required): a sequence of arguments separated by spaces: existing column name, new column name, new data type of a column,
date expression (if a new data type is `date`). Currently supported the following data types: `string`, `integer`, `date`,
`boolean`.
- `--delimiter` (optional): separator used in the source csv file.
- example: --dir path/to/dir/ --file sample.csv --column name first_name string --column age age integer --column gender gender string --column birthday birthday date dd-MM-yyyy --delimiter |

### Launch

1. Clone the project, resolve in sbt
2. Launch in terminal (the following sbt command will launch an app to process a sample csv file stored in the project):
    - run in terminal: `sbt "run --dir src/main/resources/ --file sample.csv --column name name string --column age age integer --column gender gender string --column birthday birthday date dd-MM-yyyy --delimiter ,"` 
3. Launch in IntelliJ Idea: 
- create a new Application Configuration
- `Main class`: net.learningclub.csvprofiler.Boot
- `Program arguments`: put all necessary opts like in the example above
- click `Run 'Boot'`

### Unit tests
* Run in terminal: `sbt clean test`