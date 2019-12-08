#!/bin/sh

# Before starting that script the Flume configuration should properly set

sudo mkdir /home/ergast
sudo mkdir /home/ergast/input_files

# Run configured with flume-agent.conf flume agent
flume-ng agent --conf-file /usr/lib/flume-ng/conf/log4j.properties --name agent1 --conf /usr/lib/fume-ng/conf -Dflume.root.logger=INFO,console