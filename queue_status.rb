require 'time'
require 'httparty'

def send_slack_message(msg)
  HTTParty.post("https://slack.com/api/chat.postMessage", headers: {"Authorization": "Bearer #{ENV['SLACK_TOKEN']}"}, body: {"text": msg, "channel": "tim-test", "as_user": true})
end

def wait_threshold
  ENV['WAIT_THRESHOLD'] || 5
end

def future_wait_threshold
  ENV['FUTURE_WAIT_THRESHOLD'] || 5
end

# slurm gives remaining job times in the following formats:
# "minutes", "minutes:seconds", "hours:minutes:seconds", "days-hours",
# "days-hours:minutes" and "days-hours:minutes:seconds".
# slurm gives remaining job times in the following formats:
# "minutes", "minutes:seconds", "hours:minutes:seconds", "days-hours",
# "days-hours:minutes" and "days-hours:minutes:seconds".
def determine_time(amount)
  return -1 if amount == "UNLIMITED" || amount == "NOT_SET"

  days = false
  seconds = 0
  if amount.include?("-")
    amount = amount.split("-")
    return -1 if amount[0].to_i > 300 # don't include jobs where slurm decides it will take a year

    days = true
    seconds += amount[0].to_i * 24 * 60 * 60 # days
    amount = amount[1]
  end
  amount = amount.split(":")
  if amount.length == 1
    if days
      seconds += amount[0].to_i * 60 * 60 # hours
    else
      seconds += amount[0].to_i * 60 # minutes
    end
  elsif amount.length == 2
    if days
      seconds += amount[0].to_i * 60 * 60 # hours
      seconds += amount[1].to_i * 60 # minutes
    else
      seconds += amount[0].to_i * 60 # minutes
      seconds += amount[1].to_i # seconds
    end
  else
    seconds += amount[0].to_i * 60 * 60 # hours
    seconds += amount[1].to_i * 60 # minutes
    seconds += amount[2].to_i # seconds
  end
  seconds / 60
end

# determine partitions
partitions = {}
data = %x(/opt/flight/opt/slurm/bin/sinfo p)
result = data.gsub("*", "").split("\n")
result = result.slice(1, result.length)
result.each { |partition| partitions[partition.split(" ")[0]] = {running: [], pending: [], alive_nodes: [], dead_nodes: []} }

# determine nodes, their status and their partitions
data = %x(/opt/flight/opt/slurm/bin/sinfo -Nl)
result = data.split("\n")
result = result.slice(2, result.length)
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
result = result.slice(2, result.length)
down = []
result.each do |node|
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
data =  %x(/opt/flight/opt/slurm/bin/squeue -o '%j %A %D %c %m %T %P %V %L %r' --priority)
total_pending = 0
total_running = 0
result = data.split("\n")
result = result.slice(1, result.length)
result.reverse.each do |job|
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

# determine jobs for partitions with no available resources
jobs_no_resources = 0
partition_msg = ""
total_long_waiting = 0
total_future_waiting = 0
partitions.each do |partition, details|
  partition_msg << "#{details[:running].length} job(s) running on partition #{partition}\n"
  partition_msg << "#{details[:pending].length} job(s) pending on partition #{partition}\n"
  waiting = []
  future_waiting = []
  future_wait = 0
  details[:running].each do |job|
    time = determine_time(job[8])
    future_wait += time if time > 0
  end
  details[:pending].each do |job|
    if future_wait >= future_wait_threshold
      future_waiting << job
      total_future_waiting += 1
    end
    wait_so_far = ((Time.now - Time.parse(job[7]))/60).to_i
    if wait_so_far >= wait_threshold
      waiting << job
      total_long_waiting += 1
    end
    time = determine_time(job[8])
    future_wait += time if time > 0
  end
  partition_msg << "#{waiting.length} job(s) have been pending for longer than #{wait_threshold}mins\n"
  partition_msg << "#{future_waiting.length} job(s) may not start within #{future_wait_threshold}mins\n"
  if !details[:alive_nodes].any?
    partition_msg << "Partition #{partition} has no available resources\n"
    jobs_no_resources += (details[:running].length + details[:pending].length)
  end
  partition_msg << "\n"
end

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
       "#{total_long_waiting} total job(s) have been pending for more than #{wait_threshold}mins\n",
       "#{total_future_waiting} total job(s) may not start within #{future_wait_threshold}mins\n",
       "#{jobs_no_resources} total job(s) with no available resources\n\n",
       partition_msg
].compact

msg = msg.join("")
slack = ARGV.include?("slack")
text = ARGV.include?("text")
if !slack && !text
  slack = true
  text = true
end
puts msg.gsub("*", "")  if text
send_slack_message(msg) if slack
