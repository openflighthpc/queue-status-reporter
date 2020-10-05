require 'time'
require 'httparty'

def send_slack_message(msg)
  HTTParty.post("https://slack.com/api/chat.postMessage", headers: {"Authorization": "Bearer #{ENV['SLACK_TOKEN']}"}, body: {"text": msg, "channel": "tim-test", "as_user": true})
end

partitions = {}
data = %x(/opt/flight/opt/slurm/bin/sinfo p)
result = data.gsub("*", "").split("\n")
result = result.slice(1, result.length)
result.each { |partition| partitions[partition.split(" ")[0]] = {running: [], pending: []} }

data = %x(/opt/flight/opt/slurm/bin/sinfo -Nl)
result = data.split("\n")
result = result.slice(2, result.length)
nodes = {}
idle = []
allocated = []
result.each do |node|
  node = node.split(" ").compact
  # record node names and their partitions
  if nodes.has_key?(node[0])
    nodes[node[0]] = nodes[node[0]] << node[2].gsub("*", "")
  else
    nodes[node[0]] = [node[2].gsub("*", "")]
  end
  idle << node[0] if node[3] == "idle"
  allocated << node[0] if (node[3] == "allocated" || node[3].include?("comp"))
end

data = %x(/opt/flight/opt/slurm/bin/sinfo -Nl --dead)
result = data.split("\n")
result = result.slice(2, result.length)
down = []
result.each { |node| down << node.split(" ").compact[0] }

idle.uniq!
allocated.uniq!
down.uniq!

data =  %x(/opt/flight/opt/slurm/bin/squeue -o '%j %A %D %c %m %T %P %V %r')
total_pending = 0
total_running = 0
result = data.split("\n")
result = result.slice(1, result.length)
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

no_jobs_in_partitions = []
nodes.each do |node, queues|
  jobs = false
  queues.each do |partition|
    jobs = true if (partitions[partition][:running].length > 0 || partitions[partition][:pending].length > 0)
    break if jobs == true
  end
  no_jobs_in_partitions << node if !jobs
end

puts
print "#{allocated.length} node(s) are allocated"
print ": #{allocated.join(", ")}" if allocated.any?
puts
print "#{idle.length} node(s) are idle"
print ": #{idle.join(", ")}" if idle.any?
puts
print "#{no_jobs_in_partitions.length} node(s) have no jobs in their partitions"
print ": #{no_jobs_in_partitions.join(", ")}" if no_jobs_in_partitions.any?
puts
print "#{down.length} node(s) are down"
print ": #{down.join(", ")}" if down.any?
puts
puts
puts "#{total_running} total job(s) running"
puts "#{total_pending} total job(s) pending"
puts

partitions.each do |partition, jobs|
  puts "#{jobs[:running].length} job(s) running on #{partition}"
  puts "#{jobs[:pending].length} job(s) pending on #{partition}"
  puts
end
