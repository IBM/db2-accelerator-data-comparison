# db2-accelerator-data-comparison
A command-line utility for comparing data between IBM Db2 for z/OS and IBM Db2 Analytics Accelerator.

## Overview

This tool helps database administrators verify data consistency between Db2 for z/OS tables and their corresponding data in the IBM Db2 Analytics Accelerator. It performs a row-by-row comparison to identify any discrepancies, ensuring that accelerated queries return the same results as those executed directly on Db2.

## Features

- **Comprehensive Data Comparison**: Compares all rows between Db2 and Accelerator tables
- **Intelligent Key Detection**: Automatically identifies the shortest unique key for efficient comparison
- **Detailed Reporting**: Lists specific rows that differ between systems
- **Configurable Limits**: Set maximum number of differences to report
- **Debug Mode**: Optional detailed logging for troubleshooting

## Prerequisites

- Java Runtime Environment (JRE) 11 or higher
- IBM Db2 JDBC driver
- Apache Commons CLI library (for command-line argument parsing)
- Network access to both Db2 for z/OS and the Accelerator

## Installation

1. Clone this repository or download the source code
2. Compile the Java files
3. Ensure the IBM Db2 JDBC driver and Apache Commons CLI library are in your classpath

## Usage

```
java com.ibm.db2.accelerator.Db2AcceleratorDataComparison [options]
```

### Command Line Options

| Option | Long Option | Description |
|--------|-------------|-------------|
| `-s` | `--table-schema` | Schema of the table to compare |
| `-n` | `--table-name` | Name of the table to compare |
| `-u` | `--user` | Username for Db2 connection |
| `-p` | `--password` | Password for Db2 connection |
| `-c` | `--connectionUrl` | Db2 connection URL (format: host:port/location) |
| `-a` | `--accelerator` | Name of the accelerator to compare with |
| `-m` | `--max-differences` | Maximum number of differences to report (default: 100) |
| `-d` | `--debug` | Enable debug mode for detailed logging |
| `-f` | `--file` | Output file for differences (optional) |

### Example

```
java com.ibm.db2.accelerator.Db2AcceleratorDataComparison \
  -s SAMPLE \
  -n EMPLOYEE \
  -u db2admin \
  -p password \
  -c 192.168.1.100:50000/DB01 \
  -a IDAA01 \
  -m 50
```

## Exit Codes

- **0**: Success - Data is synchronized between Db2 and Accelerator
- **8**: Failure - Either data differences were found or an error occurred

## How It Works

1. The tool connects to both Db2 for z/OS and the Accelerator
2. It identifies the shortest unique key for the table (or uses all columns if no key exists)
3. It queries the table from both systems with consistent ordering
4. It performs a row-by-row comparison to identify differences
5. It reports any rows that exist in one system but not the other

## Limitations

- Large tables may take significant time to compare
- The tool requires appropriate database permissions to access catalog tables
