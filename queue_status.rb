require 'time'
require 'httparty'

def send_slack_message(msg)
  HTTParty.post("https://slack.com/api/chat.postMessage", headers: {"Authorization": "Bearer #{ENV['SLACK_TOKEN']}"}, body: {"text": msg, "channel": "tim-test", "as_user": true})
end

def wait_threshold
  ENV['WAIT_THRESHOLD'] || 720
end

def wait_threshold_hours
  wait_threshold.divmod(60)[0]
end

def wait_threshold_mins
  wait_threshold.divmod(60)[1]
end

# determine partitions
partitions = {}
data = %x(/opt/flight/opt/slurm/bin/sinfo p)
result = data.gsub("*", "").split("\n")
result.shift
result.each { |partition| partitions[partition.split(" ")[0]] = {running: [], pending: [], alive_nodes: [], dead_nodes: []} }

# determine nodes, their status and their partitions
data = %x(/opt/flight/opt/slurm/bin/sinfo -Nl)
result = data.split("\n")
result.shift(2)
nodes = {}
idle = []
allocated = []
mixed = []
result.each do |node|
  node = node.split(" ").compact
  partition_name = node[2].gsub("*", "")
  node_name = node[0]
  if nodes.has_key?(node_name)
    nodes[node_name] = nodes[node_name] << partition_name
  else
    nodes[node_name] = [partition_name]
  end
  partitions[partition_name][:alive_nodes] = (partitions[partition_name][:alive_nodes] << node_name).uniq
  idle << node_name if node[3] == "idle"
  allocated << node_name if (node[3] == "allocated" || node[3].include?("comp"))
  mixed << node_name if node[3] == "mixed"
end

# determine unresponsive nodes
data = %x(/opt/flight/opt/slurm/bin/sinfo -Nl --dead)
result = data.split("\n")
result.shift(2)
down = []
result.reverse.each do |node|
  node = node.split(" ").compact
  partition_name = node[2].gsub("*", "")
  node_name = node[0]
  down << node_name
  alive = partitions[partition_name][:alive_nodes]
  alive.delete(node_name)
  partitions[partition_name][:alive_nodes] = alive
  partitions[partition_name][:dead_nodes] = partitions[partition_name][:dead_nodes] << node_name
end

idle.uniq!
allocated.uniq!
down.uniq!

# determine jobs, their status and partitions
data =  %x(/opt/flight/opt/slurm/bin/squeue -o '%j %A %D %c %m %T %P %V %L %l %S %e %r' --priority)
total_pending = 0
total_running = 0
result = data.split("\n")
result.shift
result.each do |job|
  job = job.split(" ").compact
  if job[5] == "PENDING"
    partitions[job[6]][:pending] = partitions[job[6]][:pending] << job
    total_pending += 1
  elsif job[5] == "RUNNING"
    partitions[job[6]][:running] = partitions[job[6]][:running] << job
    total_running += 1
  end
end

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

# determine jobs per partition
jobs_no_resources = []
partition_msg = ""
total_long_waiting = []
total_cant_determine_wait = []
final_job_end = nil
final_job_end_valid = true
partitions.each do |partition, details|
  partition_msg << "*Partition #{partition}*\n"
  partition_msg << "#{details[:running].length} job(s) running on partition #{partition}\n"
  partition_msg << "#{details[:pending].length} job(s) pending on partition #{partition}\n"

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
      if estimated_start
        wait = (estimated_start - Time.now) / 60.0
        wait = nil if wait > (300 * 24 * 60) # ignore jobs slurm decides will take 1 year to start
      end

      # flag job as long waiting if wait exceeds threshold. If no estimated start from slurm, use
      # time since job was submitted, as could already have been pending for a long period.
      if wait && wait >= wait_threshold || ((Time.now - Time.parse(job[7])) / 60.0) >= wait_threshold
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

    # record if an unbroken series of valid end times and value of the latest valid end date across all partitions
    final_job_end_valid = false if (details[:pending].any? || details[:running].any?) && !all_end_times_valid
    final_job_end = last_job_end_time if last_job_end_time && (!final_job_end || last_job_end_time > final_job_end)

    # show if jobs with unknown start times for this partition
    if cant_determine_wait.length > 0 && cant_determine_wait.length == details[:pending].length
      partition_msg << "Insufficient data to estimate job start times\n"
    elsif details[:pending].any?
      partition_msg << "#{waiting.length} job(s) estimated not to start within #{wait_threshold_hours}hrs #{wait_threshold_mins}m after submission"
      partition_msg << ": #{waiting.map {|job| job[1] }.join(", ") }" if waiting.any?
      partition_msg << "\n"
      if cant_determine_wait.any?
        partition_msg << "Insufficient data to estimate job start times for #{cant_determine_wait.length} job(s)"
        partition_msg << ": #{cant_determine_wait.map {|job| job[1] }.join(", ") }\n"
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
    final_job_end_valid = false
    partition_msg << ":awooga:Partition #{partition} has no available resources:awooga:\n"
    jobs = details[:running] + details[:pending]
    partition_msg << "Impacts jobs: " if jobs.any?
    partition_msg << "#{jobs.map { |job| job[1] }.join(", ") }" if jobs.any?
    jobs_no_resources += jobs
  end
  partition_msg << "\n"
end

# nodes and job totals
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
       "#{total_pending} total job(s) pending\n",
       "#{total_long_waiting.length} total job(s) estimated not to start within #{wait_threshold_hours}hrs #{wait_threshold_mins}m after submission",
       (": #{total_long_waiting.map {|job| job[1] }.join(", ") }"  if total_long_waiting.any?),
       "\n",
       ("Insufficient data to estimate job start times for #{total_cant_determine_wait.length} job(s)" if total_cant_determine_wait.any?),
       (": #{total_cant_determine_wait.map {|job| job[1] }.join(", ") }\n" if total_cant_determine_wait.any?),
       "#{":awooga:" if jobs_no_resources.any?}#{jobs_no_resources.length} total job(s) with no available resources#{":awooga:" if jobs_no_resources.any?}",
       (": #{jobs_no_resources.map {|job| job[1] }.join(", ") }"  if jobs_no_resources.any?),
       "\n",
       ("Estimated time all jobs completed: #{final_job_end}" if (total_running + total_pending) > 0 && final_job_end_valid),
       ("Insufficient data to estimate time all jobs completed" if (total_running + total_pending) > 0 && !final_job_end_valid),
       (". Last known end time: #{final_job_end}" if final_job_end && !final_job_end_valid),
       "\n\n",
       partition_msg
].compact

msg = msg.join("")
slack = ARGV.include?("slack")
text = ARGV.include?("text")
if !slack && !text
  slack = true
  text = true
end
puts msg.gsub("*", "").gsub(":awooga:", "")  if text
send_slack_message(msg) if slack
