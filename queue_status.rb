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
mixed.uniq!
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

# determine jobs for partitions with no available resources
jobs_no_resources = []
partition_msg = ""
total_long_waiting = []
partitions.each do |partition, details|
  partition_msg << "#{details[:running].length} job(s) running on partition #{partition}\n"
  partition_msg << "#{details[:pending].length} job(s) pending on partition #{partition}\n"
  waiting = []
  details[:pending].each do |job|
    estimated_start = Time.parse(job[10]) rescue nil
    if estimated_start
      wait = (estimated_start - Time.now) / 60.0
      wait = wait > (300 * 24 * 60) ? nil : wait
    end
    wait ||= (Time.now - Time.parse(job[7])) / 60.0
    if wait >= wait_threshold
      waiting << job
      total_long_waiting << job
    end
  end
  partition_msg << "#{waiting.length} job(s) estimated not to start within #{wait_threshold_hours}hrs #{wait_threshold_mins}m after submission"
  partition_msg << ": #{waiting.map {|job| job[1] }.join(", ") }" if waiting.any?
  partition_msg << "\n"

  if !details[:alive_nodes].any?
    partition_msg << ":awooga:Partition #{partition} has no available resources:awooga:\n"
    jobs = details[:running] + details[:pending]
    partition_msg << "Impacts jobs: " if jobs.any?
    partition_msg << "#{jobs.map { |job| job[1] }.join(", ") }" if jobs.any?
    jobs_no_resources += jobs
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
       "#{total_long_waiting.length} total job(s) estimated not to start within #{wait_threshold_hours}hrs #{wait_threshold_mins}m after submission",
       (": #{total_long_waiting.map {|job| job[1] }.join(", ") }"  if total_long_waiting.any?),
       "\n",
       "#{":awooga:" if jobs_no_resources.any?}#{jobs_no_resources.length} total job(s) with no available resources#{":awooga:" if jobs_no_resources.any?}",
       (": #{jobs_no_resources.map {|job| job[1] }.join(", ") }"  if jobs_no_resources.any?),
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
