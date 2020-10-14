# queue-status-reporter

Reporting tool for the status of jobs and partitions on a SLURM instance.

## Overview

queue-status-reporter is a Ruby application designed to gather information from a SLURM controller daemon
on jobs that are currently in the queue, and nodes/partitions that are/aren't being utilised.

## Installation

This application requires Ruby (2.5.1) and a recent version of Bundler (2.1.4).

After downloading the source code (via git or other means), the gems need to be installed using bundler:
```
cd /path/to/source
bundle install
```

## Configuration

### Slack

This application has the functionality to post any report outputs to Slack, as well as the command line.
To use this function, a [Slack bot](https://slack.com/apps/A0F7YS25R-bots) must be created in the desired recipient server. The bot's API token should then be set as an environment variable on the system running this application:

`SLACK_TOKEN=yourtoken ruby -e 'p ENV["SLACK_TOKEN"]'`

The created bot must be a member of the Slack channel you wish to post it in.

### Thresholds

This application makes use of an arbitrary time frame called a 'threshold'. The threshold is used to communicate an amount of time that is considered long in the scope of the jobs being run. There are two threshold variables used in this application: `RUN_THRESHOLD` and `WAIT_THRESHOLD`. `RUN_THRESHOLD` is used when identifying jobs that have been running for a 'long' time, while `WAIT_THRESHOLD` is used to identify jobs that have been queued for a 'long' time.

By default, `WAIT_THRESHOLD` is set to 720 minutes (12 hours) and `RUN_THRESHOLD` is set to 10080 minutes (168 hours / 7 days). Both of these values can be changed by setting them as environment variables. For example, to set the value of `WAIT_THRESHOLD` TO 60 minutes:

`WAIT_THRESHOLD=60 ruby -e 'p ENV["WAIT_THRESHOLD"]'`

## Operation

The application has a single entrypoint, `ruby queue_status.rb`, with three optional parameters to be specified at execution. The information that will be returned will resemble the following:

```
2020-10-13 12:00:00
2 node(s) are allocated: node01, node02
0 node(s) are idle
0 node(s) are mixed (some CPUs in use, some idle)
0 active node(s) have no jobs in any of their partitions
0 node(s) are down

2 total job(s) running
0 total job(s) have been running for more than 7days
3 total job(s) pending
0 total job(s) with no available resources
Insufficient data to estimate job start times
Insufficient data to estimate time all jobs completed. Latest known end time: 2021-10-13 15:20:30 +0100

Partition all
0 job(s) running on partition all
1 job(s) pending on partition all
Insufficient data to estimate job start times
Insufficient data to estimate time all jobs completed

Partition one
1 job(s) running on partition one
0 job(s) have been running for more than 7days
2 job(s) pending on partition one
2 of these pending jobs exist on at least one other partition
Insufficient data to estimate job start times
Insufficient data to estimate time all jobs completed. Latest known end time: 2021-10-13 15:20:30 +0100

Partition two
1 job(s) running on partition two
0 job(s) have been running for more than 7days
2 job(s) pending on partition two
2 of these pending jobs exist on at least one other partition
Insufficient data to estimate job start times
Insufficient data to estimate time all jobs completed

```

### Glossary

|      Term | Description                                            |
|----------:|--------------------------------------------------------|
| Allocated | This node is currently running a job                   |
| Down      | This node is switched off                              |
| Idle      | This node is doing nothing and is free to accept a job |
| Pending   | This job is in a queue, waiting to be accepted         |
| Mixed     | This node has some CPUs in use, and some not.          |

### Start/end prediction

In order for the row containing `Insufficient data to estimate job start times` to display accurate data, appropriate details should be used when submitting the job to SLURM. Specifying a `--time` flag on as many jobs/partitions as possible will increase the accuracy of the estimated job start times. Similarly, the `Insufficient data to estimate time all jobs completed` row will be affected by the `--time` flag, although some estimation (however inaccurate) is still possible.

### Command line arguments

The three optional command line arguments are as follows:

`ids` - If included, the application will display job IDs for rows that describe individual jobs.\
`slack` - If included, the application will only send the output to Slack.\
`text` - If included, the application will only print the output to the command line, ignoring any Slack specifications.\

Please note that if neither `slack` nor `text` are specified at execution, the application will execute as if both were specified.

# Contributing

Fork the project. Make your feature addition or bug fix. Send a pull
request. Bonus points for topic branches.

Read [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

# Copyright and License

Eclipse Public License 2.0, see [LICENSE.txt](LICENSE.txt) for details.

Copyright (C) 2020-present Alces Flight Ltd.

This program and the accompanying materials are made available under
the terms of the Eclipse Public License 2.0 which is available at
[https://www.eclipse.org/legal/epl-2.0](https://www.eclipse.org/legal/epl-2.0),
or alternative license terms made available by Alces Flight Ltd -
please direct inquiries about licensing to
[licensing@alces-flight.com](mailto:licensing@alces-flight.com).

queue-status-reporter is distributed in the hope that it will be
useful, but WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER
EXPRESS OR IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR
CONDITIONS OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR
A PARTICULAR PURPOSE. See the [Eclipse Public License 2.0](https://opensource.org/licenses/EPL-2.0) for more
details.
