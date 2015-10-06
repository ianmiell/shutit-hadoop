"""ShutIt module. See http://shutit.tk
"""

from shutit_module import ShutItModule


class hadoop_in_practice(ShutItModule):


	def build(self, shutit):
		# Some useful API calls for reference. See shutit's docs for more info and options:
		#
		# ISSUING BASH COMMANDS
		# shutit.send(send,expect=<default>) - Send a command, wait for expect (string or compiled regexp)
		#                                      to be seen before continuing. By default this is managed
		#                                      by ShutIt with shell prompts.
		# shutit.multisend(send,send_dict)   - Send a command, dict contains {expect1:response1,expect2:response2,...}
		# shutit.send_and_get_output(send)   - Returns the output of the sent command
		# shutit.send_and_match_output(send, matches) 
		#                                    - Returns True if any lines in output match any of 
		#                                      the regexp strings in the matches list
		# shutit.send_until(send,regexps)    - Send command over and over until one of the regexps seen in the output.
		# shutit.run_script(script)          - Run the passed-in string as a script
		# shutit.install(package)            - Install a package
		# shutit.remove(package)             - Remove a package
		# shutit.login(user='root', command='su -')
		#                                    - Log user in with given command, and set up prompt and expects.
		#                                      Use this if your env (or more specifically, prompt) changes at all,
		#                                      eg reboot, bash, ssh
		# shutit.logout(command='exit')      - Clean up from a login.
		# 
		# COMMAND HELPER FUNCTIONS
		# shutit.add_to_bashrc(line)         - Add a line to bashrc
		# shutit.get_url(fname, locations)   - Get a file via url from locations specified in a list
		# shutit.get_ip_address()            - Returns the ip address of the target
		# shutit.command_available(command)  - Returns true if the command is available to run
		#
		# LOGGING AND DEBUG
		# shutit.log(msg,add_final_message=False) -
		#                                      Send a message to the log. add_final_message adds message to
		#                                      output at end of build
		# shutit.pause_point(msg='')         - Give control of the terminal to the user
		# shutit.step_through(msg='')        - Give control to the user and allow them to step through commands
		#
		# SENDING FILES/TEXT
		# shutit.send_file(path, contents)   - Send file to path on target with given contents as a string
		# shutit.send_host_file(path, hostfilepath)
		#                                    - Send file from host machine to path on the target
		# shutit.send_host_dir(path, hostfilepath)
		#                                    - Send directory and contents to path on the target
		# shutit.insert_text(text, fname, pattern)
		#                                    - Insert text into file fname after the first occurrence of 
		#                                      regexp pattern.
		# shutit.delete_text(text, fname, pattern)
		#                                    - Delete text from file fname after the first occurrence of
		#                                      regexp pattern.
		# shutit.replace_text(text, fname, pattern)
		#                                    - Replace text from file fname after the first occurrence of
		#                                      regexp pattern.
		# ENVIRONMENT QUERYING
		# shutit.host_file_exists(filename, directory=False)
		#                                    - Returns True if file exists on host
		# shutit.file_exists(filename, directory=False)
		#                                    - Returns True if file exists on target
		# shutit.user_exists(user)           - Returns True if the user exists on the target
		# shutit.package_installed(package)  - Returns True if the package exists on the target
		# shutit.set_password(password, user='')
		#                                    - Set password for a given user on target
		#
		# USER INTERACTION
		# shutit.get_input(msg,default,valid[],boolean?,ispass?)
		#                                    - Get input from user and return output
		# shutit.fail(msg)                   - Fail the program and exit with status 1
		# 

		#The following instructions are for users who want to install the tarball version of the
		#vanilla Apache Hadoop distribution. This is a a pseudo-distributed setup and not for a
		#multi-node cluster.
		shutit.install('git')
		shutit.send('cd /usr/local')
		#First you'll need to download the tarball from the Apache downloads page at
		#http://hadoop.apache.org/common/releases.html#Download
		#and extract the tar-ball under /usr/local:
		version = '2.6.1'
		fname = 'hadoop-' + version + '.tar.gz'
		shutit.get_url(fname,['http://mirrors.ukfast.co.uk/sites/ftp.apache.org/hadoop/common/hadoop-2.6.1','http://mirror.ox.ac.uk/sites/rsync.apache.org/hadoop/common/hadoop-' + version])
		shutit.send('tar -xzf ' + fname)
		shutit.send('ln -s hadoop-' + version + ' hadoop')
		# TODO: another user?
		shutit.send('chown -R root: /usr/local/hadoop*')
		shutit.send('mkdir /usr/local/hadoop/tmp')
		shutit.send_file('/usr/local/hadoop/etc/hadoop/core-site.xml','''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>hadoop.tmp.dir</name>
<value>/usr/local/hadoop/tmp</value>
</property>
<property>
<name>fs.default.name</name>
<value>hdfs://localhost:8020</value>
</property>
</configuration>''')
		shutit.send_file('/usr/local/hadoop/etc/hadoop/hdfs-site.xml','''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>dfs.replication</name>
<value>1</value>
</property>
</configuration>''')
		shutit.send_file('/usr/local/hadoop/etc/hadoop/mapred-site.xml','''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
</configuration>''')
		shutit.send_file('/usr/local/hadoop/etc/hadoop/yarn-site.xml','''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
<description>Shuffle service that needs to be set for
Map Reduce to run.</description>
</property>
<property>
<name>yarn.log-aggregation-enable</name>
<value>true</value>
</property>
<property>
<name>yarn.log-aggregation.retain-seconds</name>
<value>2592000</value>
</property>
<property>
<name>yarn.log.server.url</name>
<value>http://0.0.0.0:19888/jobhistory/logs/</value>
</property>
<property>
<name>yarn.nodemanager.delete.debug-delay-sec</name>
<value>-1</value>
<description>Amount of time in seconds to wait before
deleting container resources.</description>
</property>
</configuration>''')

		#Set up SSH
		#Hadoop uses Secure Shell ( SSH ) to remotely launch processes such as the DataNode and TaskTracker, even when everything is running on a single node in pseudo-distributed mode. If you don't already have an SSH key pair, create one with the following command:
		shutit.send('ssh-keygen -b 2048 -t rsa',{'save the key':'','Enter passphrase':'','Enter same pass':''})
		#You'll need to copy the .ssh/id_rsa file to the authorized_keys file:
		shutit.send('cp ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys')	
		# You'll also need an SSH agent running so that you aren't prompted to enter your pass-
		# word a bazillion times when starting and stopping Hadoop. Different operating sys-
		# tems have different ways of running an SSH agent, and there are details online for
		# Cent OS and other Red Hat derivatives 2 and for OS X. 3 Google is your friend if you're
		# running on a different system.
		# To verify that the agent is running and has your keys loaded, try opening an SSH
		# connection to the local system. If you're prompted for a password, the agent's not running or doesn't have your keys
		# loaded.
		# See the Red Hat Deployment Guide section on "Configuring ssh-agent" at www.centos.org/docs/5/html/
		# 5.2/Deployment_Guide/s3-openssh-config-ssh-agent.html.
		# See Using SSH Agent With Mac OS X Leopard at www-uxsup.csx.cam.ac.uk/~aia21/osx/leopard-ssh.html.
		shutit.login(command='ssh 127.0.0.1')
		shutit.logout()
		# You need a current version of Java (1.6 or newer) installed on your system. You'll need
		# to ensure that the system path includes the binary directory of your Java installation.
		# Alternatively, you can edit /usr/local/hadoop/conf/hadoop-env.sh, uncomment the
		# JAVA_HOME line, and update the value with the location of your Java installation.
		shutit.install('openjdk-7-jdk')
		# Environment settings
		shutit.add_to_bashrc('HADOOP_HOME=/usr/local/hadoop')
		shutit.add_to_bashrc('JAVA_HOME=/usr')
		shutit.add_to_bashrc('PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin')
		shutit.add_to_bashrc('export PATH JAVA_HOME')
		shutit.send('export HADOOP_HOME=/usr/local/hadoop')
		shutit.send('export JAVA_HOME=/usr')
		shutit.send('export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin')
		# Format HDFS
		# Next you need to format HDFS . The rest of the commands in this section assume that
		# the Hadoop binary directory exists in your path, as per the preceding instructions. On
		shutit.send('hdfs namenode -format')
		# After HDFS has been formatted, you're ready to start Hadoop.
		shutit.send('yarn-daemon.sh start resourcemanager')
		shutit.send('yarn-daemon.sh start nodemanager')
		shutit.send('hadoop-daemon.sh start namenode')
		shutit.send('hadoop-daemon.sh start datanode')
		shutit.send('mr-jobhistory-daemon.sh start historyserver')
#Once Hadoop is up and running, the first thing you'll want to do is create a home
#directory for your user. If you're running on Hadoop 1, the command is
#$ hadoop fs -mkdir /user/<your-linux-username>
#On Hadoop 2, you'll run
#$ hdfs dfs -mkdir -p /user/<your-linux-username>
#Verifying the installation

		#The following commands can be used to test your Hadoop installation. The first two commands create a directory in HDFS and create a file in HDFS :
		shutit.send('hadoop fs -mkdir /user')
		shutit.send('hadoop fs -mkdir /user/root')
		shutit.send('echo "the cat sat on the mat" | hadoop fs -put - /user/root/input.txt')
		shutit.send('hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/*-examples*.jar wordcount /user/root/input.txt /user/root/output')
#Examine and verify the MapReduce job outputs on HDFS (the outputs will differ based on the contents of the config files that you used for the job inputs):
#$ hadoop fs -cat /tmp/output/part*
#at
#1
#mat
#1
#on
#1
#sat
#1
#the
#2
		shutit.send('git clone https://github.com/alexholmes/hiped2')
		shutit.send('wget -qO- https://github.com/alexholmes/hiped2/releases/download/v2.0.8/hip-2.0.0-package.tar.gz | tar -zxvf -')
		shutit.send('cd hip-2.0.0')
		shutit.send('export HIP_HOME=/usr/local/hip-2.0.0')
		shutit.send('export PATH=${PATH}:${HIP_HOME}/bin')
		shutit.send('hadoop fs -mkdir -p hip/input')
		shutit.send('echo "cat sat mat" | hadoop fs -put - hip/input/1.txt')
		shutit.send('echo "dog lay mat" | hadoop fs -put - hip/input/2.txt')
		# run the inverted index example
		shutit.send('hip hip.ch1.InvertedIndexJob --input hip/input --output hip/output')
		# examine the results in HDFS
		shutit.send('hadoop fs -cat hip/output/part*')
		shutit.pause_point('Have a shell - appendix done, back to page 21.')
		return True

	def get_config(self, shutit):
		# CONFIGURATION
		# shutit.get_config(module_id,option,default=None,boolean=False)
		#                                    - Get configuration value, boolean indicates whether the item is 
		#                                      a boolean type, eg get the config with:
		# shutit.get_config(self.module_id, 'myconfig', default='a value')
		#                                      and reference in your code with:
		# shutit.cfg[self.module_id]['myconfig']
		return True

	def test(self, shutit):
		# For test cycle part of the ShutIt build.
		return True

	def finalize(self, shutit):
		# Any cleanup required at the end.
		return True
	
	def is_installed(self, shutit):
		return False

	def stop(self, shutit):
#Stopping Hadoop 2
#To stop Hadoop 2, use the following commands:
#mr-jobhistory-daemon.sh stop historyserver
#hadoop-daemon.sh stop datanode
#hadoop-daemon.sh stop namenode
#yarn-daemon.sh stop nodemanager
#yarn-daemon.sh stop resourcemanager
		return True


def module():
	return hadoop_in_practice(
		'shutit.hadoop_in_practice.hadoop_in_practice.hadoop_in_practice', 942917995.00,
		description='',
		maintainer='',
		delivery_methods=['docker'],
		depends=['shutit.tk.setup','shutit.tk.ssh_server.ssh_server']
	)

