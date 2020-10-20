#==============================================================================
# Copyright (C) 2020-present Alces Flight Ltd.
#
# This file is part of queue-status-reporter.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License 2.0 which is available at
# <https://www.eclipse.org/legal/epl-2.0>, or alternative license
# terms made available by Alces Flight Ltd - please direct inquiries
# about licensing to licensing@alces-flight.com.
#
# queue-status-reporter is distributed in the hope that it will be useful, but
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR
# IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS
# OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A
# PARTICULAR PURPOSE. See the Eclipse Public License 2.0 for more
# details.
#
# You should have received a copy of the Eclipse Public License 2.0
# along with queue-status-reporter. If not, see:
#
#  https://opensource.org/licenses/EPL-2.0
#
# For more information on queue-status-reporter, please visit:
# https://github.com/openflighthpc/queue-status-reporter
#==============================================================================

require 'time'
require 'httparty'

def send_slack_message(msg)
  HTTParty.post("https://slack.com/api/chat.postMessage", headers: {"Authorization": "Bearer #{ENV['SLACK_TOKEN']}"}, body: {"text": msg, "channel": ENV['SLACK_CHANNEL'], "as_user": true})
end

def wait_threshold
  (ENV['WAIT_THRESHOLD'] || 720).to_i
end

def running_threshold
  (ENV['RUN_THRESHOLD'] || 10080).to_i
end

# Convert an integer holding a number of minutes
# to a human readable format
def humanise_seconds(mins)
  [[60, :m], [24, :hr], [Float::INFINITY, :day]].map do |count, name|
    if mins > 0
      mins, n = mins.divmod(count)
      n = n.to_i
      "".tap do |s|
        unless n == 0
          s << "#{n}#{name}"
          if n > 1 && (name == :hr || name == :day)
            s << 's'
          end
        end
      end
    end
  end.compact.reverse.join(' ')
end

def gather_partitions
  # parse raw output from `sinfo p`,
  # disregard header row,
  # create and populate hash with one key per row in command line data
  data = %x(sinfo p)
  split_data = data.gsub("*", "").split("\n")[1..-1]

  {}.tap do |h|
    split_data.each do |part|
      row = part.split(" ")
      h[row[0]] = {
        running: [],
        pending: [],
        alive_nodes: [],
        dead_nodes: []
      }
    end
  end
end

def node_details
  data = %x(sinfo -Nl)
  split_data = data.split("\n")[2..-1]
  nodes = {}
  idle = []
  allocated = []
  mixed = []
  down = []
  split_data.each do |node|
    node = node.split(" ").compact
    partition_name = node[2].gsub("*", "")
    node_name = node[0]
    node_status = node[-1] == 'responding' ? 'dead' : 'alive'

    # merge nodes with new node info,
    # creating a new array if the key doesn't exist
    # and ignoring duplicate elements in the arrays
    nodes[node_name] ||= Array.new
    nodes.merge!({ node_name => [partition_name] }) do |key, oldval, newval|
      oldval | newval
    end

    if node_status == 'alive'
      if node[3] == 'idle'
        idle << node_name
      elsif node[3] == 'allocated' || node[3].include?('comp')
        allocated << node_name
      elsif node[3] == 'mixed'
        mixed << node_name
      end
    else
      down << node_name
    end

  end
  [nodes, idle, allocated, mixed, down].map(&:uniq)
end

def populate_partitions_hash(partitions,nodes,down)
  # modify partitions parameter inplace
  partitions.tap do |partitions_hash|
    nodes.each do |node, parts|
      parts.each do |p|
        if down.include?(node)
          partitions_hash[p][:dead_nodes].append(node).uniq
        else
          partitions_hash[p][:alive_nodes].append(node).uniq
        end
      end
    end
  end
end

def gather_job_details(partitions)
  data = %x(squeue -o '%j %A %D %c %m %T %P %V %L %l %S %e %r' --priority)
  total_running = 0
  split_data = data.split("\n")[1..-1]
  partitions.tap do |h|
    split_data.each do |row|
      job = row.split(" ").compact
      if job[5] == "PENDING"
        h[job[6]][:pending].append(job)
      elsif job[5] == "RUNNING"
        h[job[6]][:running].append(job)
        total_running += 1
      end
    end
  end
  total_running
end

# Construct a hash to store command line arguments.
#
# This implementation allows for arguments of the form:
# ruby queue_status.rb show_ids another_var=not_true
#
# Simple boolean arguments are referenced with:
# user_args.key?('arg_name')
#
# Arguments with defined values (currently none) are referenced with:
# user_args['arg_name']
#
user_args = Hash[ ARGV.join(' ').scan(/([^=\s]+)(?:=(\S+))?/) ]

# Determine partitions
partitions = gather_partitions

# Determine node names, statuses, and their partitions
nodes, idle, allocated, mixed, down = node_details

# Populate partitions hash with new node data
populate_partitions_hash(partitions,nodes,down)

# Determine jobs, their status, and partitions
total_running = gather_job_details(partitions)
total_pending = partitions.map { |k,v| v[:pending].map { |x| x[1] } }.flatten.uniq.count

# determine nodes with no jobs in any of their partitions
no_jobs_in_partitions = []
nodes.each do |node, queues|
  if !down.include?(node)
    jobs = false
    queues.each do |partition|
      jobs = true if (partitions[partition][:running].length > 0 || partitions[partition][:pending].length > 0)
      break if jobs == true
    end
    no_jobs_in_partitions << node if !jobs
  end
end

def duplicates(list, part, index)
  # Return a list of jobs on the given partition where each job exists in at least one other partition

  new = list.dup.tap { |l| l.delete_at(index) } # create copy of partitions list sans the current partition
  unions = new.map { |p| p & part } # find the union (&) of partition 'part' with each other set in 'new'
  return unions.reduce(:|) # find the intersection (|) of the unions
end

# determine jobs per partition
jobs_no_resources = []
partition_msg = ""
total_long_waiting = []
total_long_running = []
total_cant_determine_wait = []
final_job_end = nil
final_job_end_valid = true
all_pending_ids = partitions.map { |k,v| v[:pending].map { |x| x[1] } }
partitions.each_with_index do |(partition, details), index|
  partition_msg << "*Partition #{partition}*\n"
  long_running = []
  details[:running].each do |job|
    start = Time.parse(job[10])
    if (Time.now - start) / 60.0 >= running_threshold
      long_running << job
      total_long_running << job
    end
  end
  long_running.sort_by! { |job| job[1] }
  partition_msg << "#{details[:running].length} job(s) running on partition #{partition}\n"
  if details[:running].any?
    partition_msg << "#{long_running.length} job(s) have been running for more than #{humanise_seconds(running_threshold)}"
    if long_running.any? && user_args.key?('show_ids')
      partition_msg << ": #{long_running.map {|job| job[1] }.join(", ") }"
    end
    partition_msg << "\n"
  end
  partition_msg << "#{details[:pending].length} job(s) pending on partition #{partition}\n"
  duplicate_count = duplicates(all_pending_ids, all_pending_ids[index], index).length
  partition_msg << "#{duplicate_count} of these pending jobs exist on at least one other partition\n" if duplicate_count > 0
  # only calculate times if partition has resources, as otherwise we know jobs are stuck
  if details[:alive_nodes].any?
    waiting = []
    cant_determine_wait = []
    last_job_end_time = nil
    all_end_times_valid = true

    details[:running].each do |job|
      estimated_end = Time.parse(job[11]) rescue nil
      all_end_times_valid = false if !estimated_end
      last_job_end_time = estimated_end if estimated_end && (!last_job_end_time || last_job_end_time && estimated_end > last_job_end_time)
    end

    details[:pending].each do |job|
      estimated_start = Time.parse(job[10]) rescue nil
      submit_time = Time.parse(job[7])
      wait = nil
      if estimated_start
        wait = (estimated_start - submit_time) / 60.0
        wait = nil if wait > (300 * 24 * 60) # ignore jobs slurm decides will take 1 year to start
      end

      # flag job as long waiting if wait exceeds threshold. If no estimated start from slurm, use
      # time since job was submitted, as could already have been pending for a long period.
      if wait && wait >= wait_threshold || ((Time.now - submit_time) / 60.0) >= wait_threshold
        waiting << job
        total_long_waiting << job
      elsif !wait
        cant_determine_wait << job
        total_cant_determine_wait << job
      end
      
      # determine if a valid end time provided by slurm. If so, use for determining latest valid
      # job end for jobs on this partition.
      estimated_end = Time.parse(job[11]) rescue nil
      if estimated_end
        estimated_end = nil if estimated_end - Time.now > (300 * 24 * 600) # ignore jobs slurm decides will take 1 year to end
        last_job_end_time = estimated_end if estimated_end && (!last_job_end_time || last_job_end_time && estimated_end > last_job_end_time)
      end
      # record if all jobs have valid end time estimates on this partition
      all_end_times_valid = false if !estimated_end
    end
    waiting.sort_by! { |job| job[1] }
    cant_determine_wait.sort_by! { |job| job[1] }

    # record if an unbroken series of valid end times and value of the latest valid end date across all partitions
    final_job_end_valid = false if (details[:pending].any? || details[:running].any?) && !all_end_times_valid
    final_job_end = last_job_end_time if last_job_end_time && (!final_job_end || last_job_end_time > final_job_end)

    # show if jobs with unknown start times for this partition
    if cant_determine_wait.length > 0 && cant_determine_wait.length == details[:pending].length
      partition_msg << "Insufficient data to estimate job start times\n"
    elsif details[:pending].any?
      partition_msg << "#{waiting.length} job(s) estimated not to start within #{humanise_seconds(wait_threshold)} after submission"
      if waiting.any? && user_args.key?('show_ids')
        partition_msg << ": #{waiting.map {|job| job[1] }.join(", ") }"
      end
      partition_msg << "\n"
      if cant_determine_wait.any?
        partition_msg << "Insufficient data to estimate job start times for #{cant_determine_wait.length} job(s)"
        partition_msg << ": #{cant_determine_wait.map {|job| job[1] }.join(", ") }" if user_args.key?('show_ids')
        partition_msg << "\n"
      end
    end

    # show estimated end of current jobs, or latest value available
    if details[:pending].any? || details[:running].any?
      if all_end_times_valid
        partition_msg << "Estimated time all jobs completed: #{last_job_end_time}\n" 
      else
        partition_msg << "Insufficient data to estimate time all jobs completed"
        partition_msg << ". Latest known end time: #{last_job_end_time}" if last_job_end_time
        partition_msg << "\n"
      end
    end

  # highlight jobs with no resources
  else
    final_job_end_valid = !(details[:pending].any? || details[:running].any?)
    partition_msg << ":awooga:Partition #{partition} has no available resources:awooga:\n"
    jobs = details[:running] + details[:pending]
    jobs.sort! { |job| job[1].to_i }
    if jobs.any? && user_args.key?('show_ids')
      partition_msg << "Impacts jobs: "
      partition_msg << "#{jobs.map { |job| job[1] }.join(", ") }" 
      partition_msg << "\n"
    end
    jobs_no_resources += jobs
  end
  partition_msg << "\n"
end

[jobs_no_resources, total_long_waiting, total_long_running, total_cant_determine_wait].each do |list|
  list.uniq! { |job| job[1] }
  list.sort_by! { |job| job[1] }
end
no_start_data = total_cant_determine_wait.any? && total_cant_determine_wait.length == total_pending

# nodes and job totals
# GETTING REVAMPED; CONDITIONALS LEFT ALONE FOR NOW :tm
msg = ["*#{Time.now.strftime("%F %T")}*\n",
       "#{allocated.length} node(s) are allocated",
       (": #{allocated.join(", ")}" if allocated.any?),
       "\n",
       "#{idle.length} node(s) are idle",
       (": #{idle.join(", ")}" if idle.any?),
       "\n",
       "#{mixed.length} node(s) are mixed (some CPUs in use, some idle)",
       (": #{mixed.join(", ")}" if mixed.any?),
       "\n",
       "#{no_jobs_in_partitions.length} active node(s) have no jobs in any of their partitions",
       (": #{no_jobs_in_partitions.join(", ")}" if no_jobs_in_partitions.any?),
       "\n",
       "#{down.length} node(s) are down",
       (": #{down.join(", ")}" if down.any?),
       "\n\n",
       "#{total_running} total job(s) running\n",
       ("#{total_long_running.length} total job(s) have been running for more than #{humanise_seconds(running_threshold)}" if total_running > 0),
       (": #{total_long_running.map {|job| job[1] }.join(", ") }" if total_long_running.any? && show_ids),
       ("\n" if total_running > 0),
       "#{total_pending} total job(s) pending\n",
       "#{":awooga:" if jobs_no_resources.any?}#{jobs_no_resources.length} total job(s) with no available resources#{":awooga:" if jobs_no_resources.any?}",
       (": #{jobs_no_resources.map {|job| job[1] }.join(", ") }"  if jobs_no_resources.any? && show_ids),
       "\n",
       ("Insufficient data to estimate job start times" if no_start_data),
       ("#{total_long_waiting.length} total job(s) estimated not to start within #{humanise_seconds(wait_threshold)} after submission" if !no_start_data),
       (": #{total_long_waiting.map {|job| job[1] }.join(", ") }"  if total_long_waiting.any? && !no_start_data && show_ids),
       ("\nInsufficient data to estimate job start times for #{total_cant_determine_wait.length} job(s)" if total_cant_determine_wait.any? && !no_start_data),
       (": #{total_cant_determine_wait.map {|job| job[1] }.join(", ") }" if total_cant_determine_wait.any? && !no_start_data && show_ids),
       ("\nEstimated time all jobs completed: #{final_job_end}" if (total_running + total_pending) > 0 && final_job_end_valid),
       ("\nInsufficient data to estimate time all jobs completed" if (total_running + total_pending) > 0 && !final_job_end_valid),
       (". Latest known end time: #{final_job_end}" if final_job_end && !final_job_end_valid),
       "\n\n",
       partition_msg
].compact

msg = msg.join("")
slack = user_args.key?('slack')
text = user_args.key?('text')
both = !(slack || text)
puts msg.gsub("*", "").gsub(":awooga:", "")  if both || text
send_slack_message(msg) if both || slack
